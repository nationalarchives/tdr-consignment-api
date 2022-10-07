package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config
import sangria.relay.{Connection, Edge, PageInfo}
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.{FileRepositoryMetadata, RedactedFiles}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.QueriedFileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{FileEdge, PaginationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.ConsignmentStatusInput
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, AllDescendantsInput, ClientSideMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.NaturalSorting.{ArrayOrdering, natural}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class FileService(fileRepository: FileRepository,
                  fileStatusRepository: FileStatusRepository,
                  consignmentRepository: ConsignmentRepository,
                  consignmentStatusService: ConsignmentStatusService,
                  ffidMetadataService: FFIDMetadataService,
                  avMetadataService: AntivirusMetadataService,
                  fileStatusService: FileStatusService,
                  fileMetadataService: FileMetadataService,
                  timeSource: TimeSource,
                  uuidSource: UUIDSource,
                  config: Config
                 )(implicit val executionContext: ExecutionContext) {

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource)

  private val fileUploadBatchSize: Int = config.getInt("fileUpload.batchSize")
  private val filePageMaxLimit: Int = config.getInt("pagination.filesMaxLimit")

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, userId: UUID): Future[List[FileMatches]] = {
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePaths = addFileAndMetadataInput.metadataInput.map(_.originalPath).toSet
    val allFileNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePaths, fileTypeIdentifier)
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(addFileAndMetadataInput.emptyDirectories.toSet, directoryTypeIdentifier)

    val row: (UUID, String, String) => FilemetadataRow = FilemetadataRow(uuidSource.uuid, _, _, now, userId, _)
    val rows: Future[List[Rows]] = fileMetadataService.getCustomMetadataValuesWithDefault.map(filePropertyValue => {
      ((allEmptyDirectoryNodes ++ allFileNodes) map {
        case (path, treeNode) =>
          val parentId = treeNode.parentPath.map(path => allFileNodes.getOrElse(path, allEmptyDirectoryNodes(path)).id)
          val fileId = treeNode.id
          val fileRow = FileRow(fileId, consignmentId, userId, now, filetype = Some(treeNode.treeNodeType), filename = Some(treeNode.name), parentid = parentId)
          val commonMetadataRows = row(fileId, path, ClientSideOriginalFilepath) ::
            filePropertyValue.map(fileProperty => row(fileId, fileProperty.propertyvalue, fileProperty.propertyname)).toList
          if (treeNode.treeNodeType.isFileType) {
            val input = addFileAndMetadataInput.metadataInput.filter(m => {
              val pathWithoutSlash = if (m.originalPath.startsWith("/")) m.originalPath.tail else m.originalPath
              pathWithoutSlash == path
            }).head

            //Add the 0 byte status here as know the file size at this point
            //DROID does not identify 0 byte files therefore cannot set this status at the FFID stage
            val zeroByteFileStatus = addFileStatusIfFileSizeIsZero(input, fileId)
            val clientChecksumStatus = addFileStatusForClientChecksum(input, fileId)
            val clientFilePathStatus = addFileStatusForClientFilePath(path, fileId)
            val clientChecks = if(zeroByteFileStatus == Success && clientChecksumStatus == Success && clientFilePathStatus == Success) {
              Completed
            } else {
              CompletedWithIssues
            }
            val fileMetadataRows = List(
              row(fileId, input.lastModified.toTimestampString, ClientSideFileLastModifiedDate),
              row(fileId, input.fileSize.toString, ClientSideFileSize),
              row(fileId, input.checksum, SHA256ClientSideChecksum)
            )
            MatchedFileRows(fileRow, fileMetadataRows ++ commonMetadataRows, input.matchId, clientChecks)
          } else {
            DirectoryRows(fileRow, commonMetadataRows)
          }
      }).toList
    })

    generateMatchedRows(consignmentId, rows)
  }

  private def generateMatchedRows(consignmentId: UUID, rows: Future[List[Rows]]): Future[List[FileMatches]] = {
    for {
      rowsWithMatchId <- rows
      _ <- rowsWithMatchId.grouped(fileUploadBatchSize).map(row => fileRepository.addFiles(row.map(_.fileRow), row.flatMap(_.metadataRows))).toList.head
      consignmentCheckStatus = rowsWithMatchId.find(_.clientChecks != Completed).map(_.clientChecks).getOrElse(Completed)
      _ <- consignmentStatusService.addConsignmentStatus(ConsignmentStatusInput(consignmentId, "ClientChecks", consignmentCheckStatus))
    } yield rowsWithMatchId.flatMap {
      case MatchedFileRows(fileRow, _, matchId, _) => FileMatches(fileRow.fileid, matchId) :: Nil
      case _ => Nil
    }
  }

  def addFileStatusIfFileSizeIsZero(input: ClientSideMetadataInput, fileId: UUID): String =
    if (input.fileSize == 0) {
      val statusRow = FilestatusRow(uuidSource.uuid, fileId, FFID, ZeroByteFile, Timestamp.from(timeSource.now))
      fileStatusRepository.addFileStatuses(List(statusRow))
      ZeroByteFile
    } else {
      Success
    }

  def addFileStatusForClientChecksum(input: ClientSideMetadataInput, fileId: UUID): String = {
    val clientChecksum = input.checksum match {
      case "" => Failed
      case _ => Success
    }
    val statusRow = FilestatusRow(uuidSource.uuid, fileId, ClientChecksum, clientChecksum, Timestamp.from(timeSource.now))
    fileStatusRepository.addFileStatuses(List(statusRow))
    clientChecksum
  }

  def addFileStatusForClientFilePath(path: String, fileId: UUID): String = {
    val clientFilePathStatus = path match {
      case "" => Failed
      case _ => Success
    }
    val statusRow = FilestatusRow(uuidSource.uuid, fileId, ClientFilePath, clientFilePathStatus, Timestamp.from(timeSource.now))
    fileStatusRepository.addFileStatuses(List(statusRow))
    clientFilePathStatus
  }

  def getAllDescendants(input: AllDescendantsInput): Future[Seq[File]] = {
    //For now only interested in basic file metadata
    fileRepository.getAllDescendants(input.parentIds).map(_.toFiles(Map(), List(), List(), Map()))
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    consignmentRepository.getConsignmentsOfFiles(fileIds)
      .map(_.map(consignmentByFile => FileOwnership(consignmentByFile._1, consignmentByFile._2.userid)))
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  def getFileMetadata(consignmentId: UUID, fileFilters: Option[FileFilters] = None, queriedFileFields: QueriedFileFields): Future[List[File]] = {
    val filters = fileFilters.getOrElse(FileFilters())
    for {
      fileAndMetadataList <- fileRepository.getFiles(consignmentId, filters)
      ffidMetadataList <- if(queriedFileFields.ffidMetadata) ffidMetadataService.getFFIDMetadata(consignmentId) else Future(Nil)
      avList <- if(queriedFileFields.antivirusMetadata) avMetadataService.getAntivirusMetadata(consignmentId) else Future(Nil)
      ffidStatus <- if(queriedFileFields.fileStatus) fileStatusService.getFileStatus(consignmentId) else Future(Map.empty[UUID, String])
      redactedFilePairs <- if(queriedFileFields.originalFilePath) fileRepository.getRedactedFilePairs(consignmentId, fileAndMetadataList.map(_._1.fileid)) else Future(Seq())
    } yield fileAndMetadataList.toFiles(avList, ffidMetadataList, ffidStatus, redactedFilePairs).toList
  }

  def getPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput]): Future[TDRConnection[File]] = {
    val input = paginationInput.getOrElse(
      throw InputDataException("No pagination input argument provided for 'paginatedFiles' field query"))
    val filters = input.fileFilters.getOrElse(FileFilters())
    val currentCursor = input.currentCursor
    val limit = input.limit.getOrElse(filePageMaxLimit)
    val offset = input.currentPage.getOrElse(0) * limit
    val maxFiles: Int = min(limit, filePageMaxLimit)

    for {
      response: Seq[FileRow] <- fileRepository.getPaginatedFiles(consignmentId, maxFiles, offset, currentCursor, filters)
      totalItems: Int <- fileRepository.countFilesInConsignment(consignmentId, filters.parentId, filters.fileTypeIdentifier)
      fileIds = Some(response.map(_.fileid).toSet)
      fileMetadata <- fileMetadataService.getFileMetadata(consignmentId, fileIds)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId, fileIds)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId, fileIds)
      ffidStatus <- fileStatusService.getFileStatus(consignmentId, fileIds)
    } yield {
      val lastCursor: Option[String] = response.lastOption.map(_.fileid.toString)
      val sortedFileRows = response.sortBy(row => natural(row.filename.getOrElse("")))
      val files: Seq[File] = sortedFileRows.toFiles(fileMetadata, avList, ffidMetadataList, ffidStatus)
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

    def toFiles(avMetadata: List[AntivirusMetadata],
                ffidMetadata: List[FFIDMetadata],
                ffidStatus: Map[UUID, String],
                redactedFiles: Seq[RedactedFiles]): Seq[File] = {
      response.groupBy(_._1).map {
        case (fr, fmr) =>
          val fileId = fr.fileid
          val metadataRows = fmr.flatMap(_._2)
          val metadataValues = convertMetadataRows(metadataRows)
          val redactedFileEntry: Option[RedactedFiles] = redactedFiles.find(_.redactedFileId == fileId)
          val originalFileResponseRow = redactedFileEntry
            .flatMap(rf => response.find(responseRow => Option(responseRow._1.fileid) == rf.fileId && responseRow._2.exists(_.propertyname == ClientSideOriginalFilepath)))
          val redactedOriginalFilePath = originalFileResponseRow.flatMap(_._2.map(_.value))
          File(
            fileId, fr.filetype, fr.filename, fr.parentid,
            metadataValues,
            ffidStatus.get(fileId),
            ffidMetadata.find(_.fileId == fileId),
            avMetadata.find(_.fileId == fileId),
            redactedOriginalFilePath,
            metadataRows.map(row => FileMetadataValue(row.propertyname, row.value)).toList
          )
      }.toSeq
    }
  }

  implicit class FileHelper(file: File) {
    def toFileEdge: FileEdge = {
      FileEdge(file, file.fileId.toString)
    }
  }

  implicit class FileRowsHelper(fileRows: Seq[FileRow]) {
    def toFiles(fileMetadata: Map[UUID, FileMetadataValues], avMetadata: List[AntivirusMetadata],
                ffidMetadata: List[FFIDMetadata], ffidStatus: Map[UUID, String]): Seq[File] = {
      fileRows.map(fr => {
        val id = fr.fileid
        val metadata = fileMetadata.getOrElse(id, FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None))
        File(
          id, fr.filetype, fr.filename, fr.parentid,
          metadata,
          ffidStatus.get(id),
          ffidMetadata.find(_.fileId == id),
          avMetadata.find(_.fileId == id)
        )
      })
    }
  }

  trait Rows {
    val fileRow: FileRow
    val metadataRows: List[FilemetadataRow]
    val clientChecks: String
  }

  case class MatchedFileRows(fileRow: FileRow, metadataRows: List[FilemetadataRow], matchId: Long, clientChecks: String) extends Rows

  case class DirectoryRows(fileRow: FileRow, metadataRows: List[FilemetadataRow], clientChecks: String = Completed) extends Rows

  case class FileOwnership(fileId: UUID, userId: UUID)

  case class TDRConnection[T](pageInfo: PageInfo, edges: Seq[Edge[T]], totalItems: Int, totalPages: Int) extends Connection[T]
}
