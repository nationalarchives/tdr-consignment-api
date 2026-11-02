package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config
import sangria.relay.{Connection, Edge, PageInfo}
import uk.gov.nationalarchives.Tables.{AvmetadataRow, ConsignmentRow, FfidmetadataRow, FfidmetadatamatchesRow, FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.FileRepositoryMetadata
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.QueriedFileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentMetadataFilter, FileEdge, PaginationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, FileCheckFailure, FileMatches, GetFileCheckFailuresInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.FileStatus
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataPropertyName.{legalStatus, metadataConfig, tdrDataLoadHeaderToPropertyMapper}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{FFID, allFileStatusTypes}
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.Reference
import uk.gov.nationalarchives.tdr.api.utils.NaturalSorting.{ArrayOrdering, natural}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.{LongUtils, TimestampUtils}
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class FileService(
    fileRepository: FileRepository,
    ffidMetadataService: FFIDMetadataService,
    avMetadataService: AntivirusMetadataService,
    fileStatusService: FileStatusService,
    fileMetadataService: FileMetadataService,
    referenceGeneratorService: ReferenceGeneratorService,
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    consignmentService: ConsignmentService,
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
      case _        => tokenUserId
    }
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePathInputs = addFileAndMetadataInput.metadataInput.map(i => TreeNodeInput(i.originalPath, Some(i.matchId))).toSet
    val emptyDirectoriesInputs = addFileAndMetadataInput.emptyDirectories.map(i => TreeNodeInput(i)).toSet
    val allFileNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePathInputs, fileTypeIdentifier)
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(emptyDirectoriesInputs, directoryTypeIdentifier)

    val row: (UUID, String, String) => FilemetadataRow = FilemetadataRow(uuidSource.uuid, _, _, now, userId, _)
    val defaultPropertyValues = metadataConfig.getPropertiesWithDefaultValue

    for {
      consignmentMetadata <- consignmentMetadataRepository.getConsignmentMetadata(consignmentId, Some(ConsignmentMetadataFilter(List(legalStatus))))
      consignment <- consignmentService.getConsignment(consignmentId)
      rows <- Future.successful(((allEmptyDirectoryNodes ++ allFileNodes) map { case (path, treeNode) =>
        val parentNode: Option[TreeNode] = treeNode.parentPath.map(path => allFileNodes.getOrElse(path, allEmptyDirectoryNodes(path)))
        val parentId = parentNode.map(_.id)
        val parentFileReference = parentNode.flatMap(_.reference)
        val fileId = treeNode.id
        val assetId = Some(uuidSource.uuid)
        val fileRow = createFileRow(userId, now, consignmentId, treeNode, parentId, parentFileReference, fileId, assetId)
        val consignmentLevelMetadata = fileMetadataService.consignmentLevelMetadata(consignment, consignmentMetadata)
        val metadataProperties = defaultPropertyValues ++ consignmentLevelMetadata
        createFileRows(addFileAndMetadataInput, row, metadataProperties, path, treeNode, parentFileReference, fileId, fileRow, assetId)
      }).toList)
      matchedFileRows <- generateMatchedRows(rows)
    } yield matchedFileRows
  }

  private def generateMatchedRows(rowsWithMatchId: List[Rows]): Future[List[FileMatches]] = {
    for {
      _ <- rowsWithMatchId.grouped(fileUploadBatchSize).map(row => fileRepository.addFiles(row.map(_.fileRow), row.flatMap(_.metadataRows))).toList.head
    } yield rowsWithMatchId.flatMap {
      case MatchedFileRows(fileRow, _, matchId) => FileMatches(fileRow.fileid, matchId) :: Nil
      case _                                    => Nil
    }
  }

  def getFileDetails(ids: Seq[UUID]): Future[Seq[FileDetails]] = {
    fileRepository.getFileFields(ids.toSet).map(_.map(f => FileDetails(f._1, f._2, f._3, f._4, f._5)))
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    fileRepository
      .getFileFields(fileIds.toSet)
      .map(_.map { case (fileId, _, userId, _, _) => FileOwnership(fileId, userId) })
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  def getFileCheckFailures(input: Option[GetFileCheckFailuresInput]): Future[Seq[FileFields.FileCheckFailure]] = {
    fileRepository
      .getFilesWithFileCheckFailures(
        consignmentId = input.flatMap(_.consignmentId),
        startDateTime = input.flatMap(_.startDateTime),
        endDateTime = input.flatMap(_.endDateTime)
      )
      .map { result =>
        fileCheckFailureResultsWithRankOverFileName(result)
          .map { case (rank, ((((((fileStatus, file), consignment), avMetadata), ffidMetadata), ffidMatches), checksumMetadata)) =>
            FileCheckFailure(
              fileId = file.fileid,
              consignmentId = consignment.consignmentid,
              consignmentType = consignment.consignmenttype,
              rankOverFilePath = rank,
              PUID = ffidMatches.flatMap(_.puid),
              userId = consignment.userid,
              statusType = fileStatus.statustype,
              statusValue = fileStatus.value,
              seriesName = consignment.seriesname,
              transferringBodyName = consignment.transferringbodyname,
              antivirusResult = avMetadata.map(_.result),
              extension = ffidMatches.flatMap(_.extension),
              identificationBasis = ffidMatches.map(_.identificationbasis),
              extensionMismatch = ffidMatches.flatMap(_.extensionmismatch).contains(true),
              formatName = ffidMatches.flatMap(_.formatname),
              checksum = checksumMetadata.map(_.value),
              createdDateTime = fileStatus.createddatetime.toZonedDateTime
            )
          }
      }
  }

  private def fileCheckFailureResultsWithRankOverFileName(
      result: Seq[((((((FilestatusRow, FileRow), ConsignmentRow), Option[AvmetadataRow]), Option[FfidmetadataRow]), Option[FfidmetadatamatchesRow]), Option[FilemetadataRow])]
  ): Seq[(Int, ((((((FilestatusRow, FileRow), ConsignmentRow), Option[AvmetadataRow]), Option[FfidmetadataRow]), Option[FfidmetadatamatchesRow]), Option[FilemetadataRow]))] = {
    val grouped = result.groupBy { case ((((((_, file), _), _), _), _), _) => file.filename }
    grouped.values.zipWithIndex.flatMap { case (group, rank) =>
      group.map(row => (rank + 1, row))
    }.toSeq
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

  def getFileIds(consignmentId: UUID): Future[Seq[UUID]] = fileRepository.getFileIds(consignmentId)

  private def getPropertyNames(fileMetadataFilters: Option[FileMetadataFilters]): Future[Seq[String]] = {
    fileMetadataFilters match {
      case Some(FileMetadataFilters(properties)) => Future(properties.getOrElse(Nil))
      case None                                  => Future(Nil)
    }
  }

  private def createFileRows(
      addFileAndMetadataInput: AddFileAndMetadataInput,
      row: (UUID, Reference, Reference) => FilemetadataRow,
      defaultPropertyValues: Map[String, String],
      path: Reference,
      treeNode: TreeNode,
      parentFileReference: Option[Reference],
      fileId: UUID,
      fileRow: FileRow,
      assetId: Option[UUID]
  ): Rows = {

    val commonMetadataRows = List(
      row(fileId, fileId.toString, FileMetadataPropertyName.uuid),
      row(fileId, path, FileMetadataPropertyName.filePath),
      row(fileId, treeNode.treeNodeType, FileMetadataPropertyName.fileType),
      row(fileId, treeNode.name, FileMetadataPropertyName.fileName),
      row(fileId, treeNode.reference.getOrElse(""), FileMetadataPropertyName.fileReference),
      row(fileId, parentFileReference.getOrElse(""), FileMetadataPropertyName.parentReference)
    ) ++ defaultPropertyValues
      .map(fileProperty => {
        row(fileId, fileProperty._2, tdrDataLoadHeaderToPropertyMapper(fileProperty._1))
      })
      .toList

    val optionalValues = if (assetId.isDefined) {
      List(row(fileId, assetId.get.toString, FileMetadataPropertyName.assetId))
    } else Nil

    if (treeNode.treeNodeType.isFileType) {
      val input = addFileAndMetadataInput.metadataInput
        .filter(m => {
          val pathWithoutSlash = if (m.originalPath.startsWith("/")) m.originalPath.tail else m.originalPath
          pathWithoutSlash == path
        })
        .head

      val fileMetadataRows = List(
        row(fileId, input.lastModified.toTimestampString, FileMetadataPropertyName.dateLastModified),
        row(fileId, input.fileSize.toString, FileMetadataPropertyName.fileSize),
        row(fileId, input.checksum, FileMetadataPropertyName.clientSideChecksum)
      )
      MatchedFileRows(fileRow, fileMetadataRows ++ commonMetadataRows ++ optionalValues, input.matchId)
    } else {
      DirectoryRows(fileRow, commonMetadataRows ++ optionalValues)
    }
  }

  private def createFileRow(
      userId: UUID,
      now: Timestamp,
      consignmentId: UUID,
      treeNode: TreeNode,
      parentId: Option[UUID],
      parentFileReference: Option[Reference],
      fileId: UUID,
      assetId: Option[UUID]
  ) = {
    FileRow(
      fileId,
      consignmentId,
      userId,
      now,
      filetype = Some(treeNode.treeNodeType),
      filename = Some(treeNode.name),
      parentid = parentId,
      filereference = treeNode.reference,
      parentreference = parentFileReference,
      uploadmatchid = treeNode.matchId,
      assetid = assetId
    )
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
      // Indexed once rather than scanned per file, which was quadratic in the size of the consignment.
      val statusesByFileId = fileStatuses.groupBy(_.fileId)
      val ffidMetadataByFileId = ffidMetadata.groupBy(_.fileId)
      val avMetadataByFileId = avMetadata.groupBy(_.fileId)

      response
        .groupBy(_._1)
        .map { case (fr, fmr) =>
          val fileId = fr.fileid
          val metadataRows = fmr.flatMap(_._2)
          val metadataValues = convertMetadataRows(metadataRows)
          val statuses = statusesByFileId.getOrElse(fileId, Nil)
          File(
            fileId,
            fr.uploadmatchid,
            fr.filetype,
            fr.filename,
            fr.filereference,
            fr.parentid,
            fr.parentreference,
            metadataValues,
            ffidStatus.get(fileId),
            ffidMetadataByFileId.get(fileId).flatMap(_.headOption),
            avMetadataByFileId.get(fileId).flatMap(_.headOption),
            fmr.find(_._2.exists(_.propertyname == "OriginalFilepath")).flatMap(_._2.map(_.value)),
            filterMetadataRows(metadataRows, metadataPropertyNames).toList,
            statuses,
            fr.assetid
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
      // Indexed once rather than scanned per file, which was quadratic in the size of the consignment.
      val statusesByFileId = fileStatuses.groupBy(_.fileId)
      val ffidMetadataByFileId = ffidMetadata.groupBy(_.fileId)
      val avMetadataByFileId = avMetadata.groupBy(_.fileId)

      fileRows.map(fr => {
        val id = fr.fileid
        val assetId = fr.assetid
        val metadata = fileMetadata.getOrElse(
          id,
          FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
        )
        val statuses = statusesByFileId.getOrElse(id, Nil)
        File(
          id,
          fr.uploadmatchid,
          fr.filetype,
          fr.filename,
          fr.filereference,
          fr.parentid,
          fr.parentreference,
          metadata,
          ffidStatus.get(id),
          ffidMetadataByFileId.get(id).flatMap(_.headOption),
          avMetadataByFileId.get(id).flatMap(_.headOption),
          fileStatuses = statuses,
          assetId = assetId
        )
      })
    }
  }

  trait Rows {
    val fileRow: FileRow
    val metadataRows: List[FilemetadataRow]
  }

  case class MatchedFileRows(fileRow: FileRow, metadataRows: List[FilemetadataRow], matchId: String) extends Rows

  case class DirectoryRows(fileRow: FileRow, metadataRows: List[FilemetadataRow]) extends Rows

  case class FileOwnership(fileId: UUID, userId: UUID)

  case class TDRConnection[T](pageInfo: PageInfo, edges: Seq[Edge[T]], totalItems: Int, totalPages: Int) extends Connection[T]

  case class FileDetails(fileId: UUID, fileType: Option[String], userId: UUID, consignmentId: UUID, uploadMatchId: Option[String])
}
