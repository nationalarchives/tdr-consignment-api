package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput, FileMatches, Files}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.FileTypeHelper
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import java.sql.Timestamp
import java.util.UUID

import com.typesafe.config.Config
import sangria.relay.{DefaultConnection, PageInfo}
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{FileEdge, PaginationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class FileService(
                   fileRepository: FileRepository,
                   consignmentRepository: ConsignmentRepository,
                   consignmentStatusRepository: ConsignmentStatusRepository,
                   fileMetadataService: FileMetadataService,
                   ffidMetadataService: FFIDMetadataService,
                   avMetadataService: AntivirusMetadataService,
                   timeSource: TimeSource,
                   uuidSource: UUIDSource,
                   config: Config
                 )(implicit val executionContext: ExecutionContext) {

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource)
  val maxLimit: Int = config.getInt("pagination.consignmentsMaxLimit")

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, userId: UUID): Future[List[FileMatches]] = {
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePaths = addFileAndMetadataInput.metadataInput.map(_.originalPath).toSet
    val allNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePaths)

    val fileRows: List[FileRow] = (allNodes map {
      case (_, treeNode) =>
        val parentId = treeNode.parentPath.map(v => allNodes(v).id)
        FileRow(treeNode.id, consignmentId, userId, now, filetype = Some(treeNode.treeNodeType), filename = Some(treeNode.name), parentid = parentId)
    }).toList

    val metadataWithIds: List[(UUID, ClientSideMetadataInput)] =
      fileRows.filter(_.filetype.get.isFileType).map(_.fileid).zip(addFileAndMetadataInput.metadataInput)
    val row: (UUID, String, String) => FilemetadataRow = FilemetadataRow(uuidSource.uuid, _, _, now, userId, _)

    val fileMetadataRows: Seq[FilemetadataRow] = metadataWithIds.flatMap {
      case (fileId, input) =>
        Seq(
          row(fileId, input.originalPath, ClientSideOriginalFilepath),
          row(fileId, input.lastModified.toTimestampString, ClientSideFileLastModifiedDate),
          row(fileId, input.fileSize.toString, ClientSideFileSize),
          row(fileId, input.checksum, SHA256ClientSideChecksum)
        ) ++ staticMetadataProperties.map(property => {
          row(fileId, property.value, property.name)
        })
      case _ => Seq()
    }
    for {
      _ <- fileRepository.addFiles(fileRows, fileMetadataRows)
    } yield metadataWithIds.map(m => (m._1, m._2.matchId)).map(f => FileMatches(f._1, f._2))
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    consignmentRepository.getConsignmentsOfFiles(fileIds)
      .map(_.map(consignmentByFile => FileOwnership(consignmentByFile._1, consignmentByFile._2.userid)))
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  def getFileMetadata(consignmentId: UUID): Future[List[File]] = {
    for {
      fileMetadataList <- fileMetadataService.getFileMetadata(consignmentId)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId)
    } yield {
      fileMetadataList map {
        case (fileId, fileMetadata) =>
          File(fileId, None, None, None, fileMetadata, ffidMetadataList.find(_.fileId == fileId), avList.find(_.fileId == fileId))
      }
    }.toList
  }

  def getFileConnections(consignmentId: UUID, parentId: Option[UUID], paginationArgs: PaginationInput): Future[DefaultConnection[File]] = {
    val limit = paginationArgs.limit
    val currentCursor = paginationArgs.currentCursor
    val maxFiles: Option[Int] = if (limit.isDefined) { Some(min(paginationArgs.limit.get, maxLimit)) } else None

    val paginatedFiles = for {
      fileRowList <- fileRepository.getFiles(consignmentId, parentId, maxFiles, currentCursor)
      fileIds = fileRowList.map(_.fileid).toSet
      fileMetadataList <- fileMetadataService.getFileMetadata(consignmentId, Some(fileIds))
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId, Some(fileIds))
      avList <- avMetadataService.getAntivirusMetadata(consignmentId, Some(fileIds))
      hasNextPage = fileRowList.nonEmpty
      lastCursor: Option[UUID] = if (hasNextPage) Some(fileRowList.last.fileid) else None
      paginatedFiles = convertToEdges(fileRowList, fileMetadataList, ffidMetadataList, avList)
    } yield PaginatedFiles(lastCursor, paginatedFiles)

    paginatedFiles.map(r => {
        val endCursor = r.lastCursor
        val edges = r.fileEdges
        DefaultConnection(
          PageInfo(
            startCursor = edges.headOption.map(_.cursor),
            endCursor = Some(endCursor.get.toString),
            hasNextPage = endCursor.isDefined,
            hasPreviousPage = paginationArgs.currentCursor.isDefined
          ),
          edges
        )
      }
    )

  }

  private def convertRowToFile(row: FileRow,
                               fileMetadata: FileMetadataValues,
                               ffidMetadata: Option[FFIDMetadata],
                               av: Option[AntivirusMetadata]): File = {
    File(row.fileid, row.filetype, row.filename, row.parentid, fileMetadata, ffidMetadata, av)
  }

  private def convertToEdges(fileRows: Seq[FileRow],
                             fileMetadata: Map[UUID, FileMetadataValues],
                             ffidMetadataList: List[FFIDMetadata],
                             avList: List[AntivirusMetadata]): Seq[FileEdge] = {
    fileRows.map(fr => convertRowToFile(
        fr,
        fileMetadata.get(fr.fileid).get,
        ffidMetadataList.find(_.fileId == fr.fileid),
        avList.find(_.fileId == fr.fileid)))
      .map(f => FileEdge(f, f.fileId.toString))
  }
}

case class FileOwnership(fileId: UUID, userId: UUID)
case class PaginatedFiles(lastCursor: Option[UUID], fileEdges: Seq[FileEdge])
