package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config
import sangria.relay.{Connection, Edge, PageInfo}
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilepropertyRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.FileRepositoryMetadata
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.QueriedFileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{FileEdge, PaginationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, AllDescendantsInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{FFID, allFileStatusTypes, defaultStatuses}
import uk.gov.nationalarchives.tdr.api.utils.NaturalSorting.{ArrayOrdering, natural}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class FileService(
    fileRepository: FileRepository,
    customMetadataPropertiesRepository: CustomMetadataPropertiesRepository,
    ffidMetadataService: FFIDMetadataService,
    avMetadataService: AntivirusMetadataService,
    fileStatusService: FileStatusService,
    fileMetadataService: FileMetadataService,
    referenceGeneratorService: ReferenceGeneratorService,
    timeSource: TimeSource,
    uuidSource: UUIDSource,
    config: Config
)(implicit val executionContext: ExecutionContext) {

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource, referenceGeneratorService, config)

  private val fileUploadBatchSize: Int = config.getInt("fileUpload.batchSize")
  private val filePageMaxLimit: Int = config.getInt("pagination.filesMaxLimit")

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, tokenUserId: UUID): Future[List[FileMatches]] = {
    val userId = addFileAndMetadataInput.userIdOverride match {
      case Some(id) => id
      case _ => tokenUserId
    }
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePaths = addFileAndMetadataInput.metadataInput.map(_.originalPath).toSet
    val allFileNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePaths, fileTypeIdentifier)
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(addFileAndMetadataInput.emptyDirectories.toSet, directoryTypeIdentifier)

    val row: (UUID, String, String) => FilemetadataRow = FilemetadataRow(uuidSource.uuid, _, _, now, userId, _)
    val rows: Future[List[Rows]] = customMetadataPropertiesRepository.getCustomMetadataValuesWithDefault.map(filePropertyValue => {
      ((allEmptyDirectoryNodes ++ allFileNodes) map { case (path, treeNode) =>
        val parentNode: Option[TreeNode] = treeNode.parentPath.map(path => allFileNodes.getOrElse(path, allEmptyDirectoryNodes(path)))
        val parentId = parentNode.map(_.id)
        val parentFileReference = parentNode.flatMap(_.reference)
        val fileId = treeNode.id
        val fileRow = FileRow(
          fileId,
          consignmentId,
          userId,
          now,
          filetype = Some(treeNode.treeNodeType),
          filename = Some(treeNode.name),
          parentid = parentId,
          filereference = treeNode.reference,
          parentreference = parentFileReference
        )

        val commonMetadataRows = List(
          row(fileId, fileId.toString, FileUUID),
          row(fileId, path, ClientSideOriginalFilepath),
          row(fileId, treeNode.treeNodeType, FileType),
          row(fileId, treeNode.name, Filename),
          row(fileId, treeNode.reference.getOrElse(""), FileReference),
          row(fileId, parentFileReference.getOrElse(""), ParentReference)
        ) ++ filePropertyValue.map(fileProperty => row(fileId, fileProperty.propertyvalue, fileProperty.propertyname)).toList

        if (treeNode.treeNodeType.isFileType) {
          val input = addFileAndMetadataInput.metadataInput
            .filter(m => {
              val pathWithoutSlash = if (m.originalPath.startsWith("/")) m.originalPath.tail else m.originalPath
              pathWithoutSlash == path
            })
            .head

          val fileMetadataRows = List(
            row(fileId, input.lastModified.toTimestampString, ClientSideFileLastModifiedDate),
            row(fileId, input.fileSize.toString, ClientSideFileSize),
            row(fileId, input.checksum, SHA256ClientSideChecksum)
          )
          MatchedFileRows(fileRow, fileMetadataRows ++ commonMetadataRows, input.matchId)
        } else {
          DirectoryRows(fileRow, commonMetadataRows)
        }
      }).toList
    })

    val matchedFileRows = generateMatchedRows(rows)
    addDefaultMetadataStatuses(matchedFileRows)
    matchedFileRows
  }

  private def addDefaultMetadataStatuses(fileMatches: Future[List[FileMatches]]): Future[List[FileStatus]] = {
    for {
      matches <- fileMatches
      fileIds = matches.map(_.fileId).toSet
      statusInputs = fileIds
        .flatMap(id => {
          defaultStatuses.map(ds => AddFileStatusInput(id, ds._1, ds._2))
        })
        .toList
      addedStatuses <- fileStatusService.addFileStatuses(AddMultipleFileStatusesInput(statusInputs))
    } yield addedStatuses
  }

  private def generateMatchedRows(rows: Future[List[Rows]]): Future[List[FileMatches]] = {
    for {
      rowsWithMatchId <- rows
      _ <- rowsWithMatchId.grouped(fileUploadBatchSize).map(row => fileRepository.addFiles(row.map(_.fileRow), row.flatMap(_.metadataRows))).toList.head
    } yield rowsWithMatchId.flatMap {
      case MatchedFileRows(fileRow, _, matchId) => FileMatches(fileRow.fileid, matchId) :: Nil
      case _                                    => Nil
    }
  }

  def getAllDescendants(input: AllDescendantsInput): Future[Seq[File]] = {
    // For now only interested in basic file metadata
    fileRepository.getAllDescendants(input.parentIds).map(_.toFiles(Map(), List(), List(), Map()))
  }

  def getFileDetails(ids: Seq[UUID]): Future[Seq[FileDetails]] = {
    fileRepository.getFileFields(ids.toSet).map(_.map(f => FileDetails(f._1, f._2, f._3, f._4)))
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    fileRepository
      .getFileFields(fileIds.toSet)
      .map(_.map { case (fileId, _, userId, _) => FileOwnership(fileId, userId) })
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  @deprecated("Use getPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput], queriedFileFields: Option[QueriedFileFields] = None)")
  def getFileMetadata(consignmentId: UUID, fileFilters: Option[FileFilters] = None, queriedFileFields: QueriedFileFields): Future[List[File]] = {
    val filters = fileFilters.getOrElse(FileFilters())
    val fileIds = filters.selectedFileIds.map(_.toSet)

    for {
      fileAndMetadataList <- fileRepository.getFiles(consignmentId, filters)
      ffidMetadataList <- if (queriedFileFields.ffidMetadata) ffidMetadataService.getFFIDMetadata(consignmentId) else Future.successful(Nil)
      avList <- if (queriedFileFields.antivirusMetadata) avMetadataService.getAntivirusMetadata(consignmentId) else Future.successful(Nil)
      propertyNames <- getPropertyNames(filters.metadataFilters)
      ffidStatuses <- if (queriedFileFields.fileStatus) fileStatusService.getFileStatuses(consignmentId, Set(FFID), fileIds) else Future.successful(Nil)
      fileStatuses <-
        if (queriedFileFields.fileStatuses) fileStatusService.getFileStatuses(consignmentId, allFileStatusTypes, fileIds) else Future.successful(Nil)
    } yield {
      val ffidStatus = ffidStatuses.map(fs => fs.fileId -> fs.statusValue).toMap
      fileAndMetadataList.toFiles(avList, ffidMetadataList, ffidStatus, propertyNames, fileStatuses).toList
    }
  }

  def getPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput], queriedFileFields: QueriedFileFields): Future[TDRConnection[File]] = {
    val input = paginationInput.getOrElse(throw InputDataException("No pagination input argument provided for 'paginatedFiles' field query"))
    val filters = input.fileFilters.getOrElse(FileFilters())
    val currentCursor = input.currentCursor
    val limit = input.limit.getOrElse(filePageMaxLimit)
    val offset = input.currentPage.getOrElse(0) * limit
    val maxFiles: Int = min(limit, filePageMaxLimit)

    for {
      response: Seq[FileRow] <- fileRepository.getPaginatedFiles(consignmentId, maxFiles, offset, currentCursor, filters)
      totalItems: Int <- fileRepository.countFilesInConsignment(consignmentId, filters.parentId, filters.fileTypeIdentifier)
      fileIds = Some(response.map(_.fileid).toSet)
      fileMetadata <- fileMetadataService.getFileMetadata(None, fileIds)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId, fileIds)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId, fileIds)
      ffidStatuses <- if (queriedFileFields.fileStatus) fileStatusService.getFileStatuses(consignmentId, Set(FFID), fileIds) else Future(Nil)
      fileStatuses <- if (queriedFileFields.fileStatuses) fileStatusService.getFileStatuses(consignmentId, allFileStatusTypes, fileIds) else Future(Nil)
    } yield {
      val lastCursor: Option[String] = response.lastOption.map(_.fileid.toString)
      val sortedFileRows = response.sortBy(row => natural(row.filename.getOrElse("")))
      val ffidStatus = ffidStatuses.map(fs => fs.fileId -> fs.statusValue).toMap
      val files: Seq[File] = sortedFileRows.toFiles(fileMetadata, avList, ffidMetadataList, ffidStatus, fileStatuses)
      val edges: Seq[FileEdge] = files.map(_.toFileEdge)
      val totalPages = Math.ceil(totalItems.toDouble / limit.toDouble).toInt
      TDRConnection(
        PageInfo(
          startCursor = edges.headOption.map(_.cursor),
          endCursor = lastCursor,
          hasNextPage = lastCursor.isDefined,
          hasPreviousPage = currentCursor.isDefined
        ),
        edges,
        totalItems,
        totalPages
      )
    }
  }

  def getConsignmentParentFolderId(consignmentId: UUID): Future[Option[UUID]] = {
    for {
      rows <- fileRepository.getConsignmentParentFolder(consignmentId)
    } yield rows.map(_.fileid).headOption
  }

  private def getPropertyNames(fileMetadataFilters: Option[FileMetadataFilters]): Future[Seq[String]] = {
    fileMetadataFilters match {
      case Some(FileMetadataFilters(closureMetadata, descriptiveMetadata, properties)) =>
        for {
          metadataProperties <- customMetadataPropertiesRepository.getCustomMetadataProperty
          closureMetadataPropertyNames = if (closureMetadata) metadataProperties.closureFields.toPropertyNames else Nil
          descriptiveMetadataPropertyNames = if (descriptiveMetadata) metadataProperties.descriptiveFields.toPropertyNames else Nil
          propertyNames = if (properties.nonEmpty) metadataProperties.additionalFields(properties).toPropertyNames else Nil
        } yield closureMetadataPropertyNames ++ descriptiveMetadataPropertyNames ++ propertyNames
      case None => Future(Nil)
    }
  }
}

object FileService {
  implicit class FileRowHelper(fr: Option[FileRow]) {
    def fileType: Option[String] = fr.flatMap(_.filetype)

    def fileName: Option[String] = fr.flatMap(_.filename)

    def parentId: Option[UUID] = fr.flatMap(_.parentid)
  }

  implicit class FileRepositoryResponseHelper(response: Seq[FileRepositoryMetadata]) {
    private def convertMetadataRows(rows: Seq[FilemetadataRow]): FileMetadataValues = {
      getFileMetadataValues(rows)
    }

    private def filterMetadataRows(rows: Seq[FilemetadataRow], metadataPropertyName: Seq[String]): Seq[FileMetadataValue] = {

      if (metadataPropertyName.isEmpty) {
        rows.map(row => FileMetadataValue(row.propertyname, row.value))
      } else {
        rows.collect { case row if metadataPropertyName.contains(row.propertyname) => FileMetadataValue(row.propertyname, row.value) }
      }
    }

    def toFiles(
        avMetadata: List[AntivirusMetadata],
        ffidMetadata: List[FFIDMetadata],
        ffidStatus: Map[UUID, String],
        metadataPropertyNames: Seq[String],
        fileStatuses: List[FileStatus]
    ): Seq[File] = {
      response
        .groupBy(_._1)
        .map { case (fr, fmr) =>
          val fileId = fr.fileid
          val metadataRows = fmr.flatMap(_._2)
          val metadataValues = convertMetadataRows(metadataRows)
          val statuses = fileStatuses.filter(_.fileId == fileId)
          File(
            fileId,
            fr.filetype,
            fr.filename,
            fr.filereference,
            fr.parentid,
            fr.parentreference,
            metadataValues,
            ffidStatus.get(fileId),
            ffidMetadata.find(_.fileId == fileId),
            avMetadata.find(_.fileId == fileId),
            fmr.find(_._2.exists(_.propertyname == "OriginalFilepath")).flatMap(_._2.map(_.value)),
            filterMetadataRows(metadataRows, metadataPropertyNames).toList,
            statuses
          )
        }
        .toSeq
    }
  }

  implicit class FileHelper(file: File) {
    def toFileEdge: FileEdge = {
      FileEdge(file, file.fileId.toString)
    }
  }

  implicit class FileRowsHelper(fileRows: Seq[FileRow]) {
    def toFiles(
        fileMetadata: Map[UUID, FileMetadataValues],
        avMetadata: List[AntivirusMetadata],
        ffidMetadata: List[FFIDMetadata],
        ffidStatus: Map[UUID, String],
        fileStatuses: List[FileStatus] = Nil
    ): Seq[File] = {
      fileRows.map(fr => {
        val id = fr.fileid
        val metadata = fileMetadata.getOrElse(id, FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None))
        val statuses = fileStatuses.filter(_.fileId == id)
        File(
          id,
          fr.filetype,
          fr.filename,
          fr.filereference,
          fr.parentid,
          fr.parentreference,
          metadata,
          ffidStatus.get(id),
          ffidMetadata.find(_.fileId == id),
          avMetadata.find(_.fileId == id),
          fileStatuses = statuses
        )
      })
    }
  }

  implicit class FilePropertyRowsHelper(fields: Seq[FilepropertyRow]) {
    def toPropertyNames: Seq[String] = fields.map(_.name)

    def closureFields: Seq[FilepropertyRow] = {
      fields.filter(f => f.propertygroup.contains("MandatoryClosure") || f.propertygroup.contains("OptionalClosure"))
    }

    def descriptiveFields: Seq[FilepropertyRow] = {
      fields.filter(f => f.propertygroup.contains("MandatoryMetadata") || f.propertygroup.contains("OptionalMetadata"))
    }

    def additionalFields(properties: Option[List[String]]): Seq[FilepropertyRow] =
      properties.map(properties => fields.filter(f => properties.contains(f.name))).getOrElse(Nil)
  }

  trait Rows {
    val fileRow: FileRow
    val metadataRows: List[FilemetadataRow]
  }

  case class MatchedFileRows(fileRow: FileRow, metadataRows: List[FilemetadataRow], matchId: Long) extends Rows

  case class DirectoryRows(fileRow: FileRow, metadataRows: List[FilemetadataRow]) extends Rows

  case class FileOwnership(fileId: UUID, userId: UUID)

  case class TDRConnection[T](pageInfo: PageInfo, edges: Seq[Edge[T]], totalItems: Int, totalPages: Int) extends Connection[T]

  case class FileDetails(fileId: UUID, fileType: Option[String], userId: UUID, consignmentId: UUID)
}
