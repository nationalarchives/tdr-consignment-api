package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config

import java.sql.Timestamp
import java.util.UUID
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.FileRepositoryMetadata
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import scala.concurrent.{ExecutionContext, Future}

class FileService(fileRepository: FileRepository,
                  consignmentRepository: ConsignmentRepository,
                  ffidMetadataService: FFIDMetadataService,
                  avMetadataService: AntivirusMetadataService,
                  fileStatusService: FileStatusService,
                  timeSource: TimeSource,
                  uuidSource: UUIDSource,
                  config: Config
                 )(implicit val executionContext: ExecutionContext) {

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource)

  private val batchSize: Int = config.getInt("fileUpload.batchSize")

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, userId: UUID): Future[List[FileMatches]] = {
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePaths = addFileAndMetadataInput.metadataInput.map(_.originalPath).toSet
    val allFileNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePaths, fileTypeIdentifier)
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(addFileAndMetadataInput.emptyDirectories.toSet, directoryTypeIdentifier)

    val row: (UUID, String, String) => FilemetadataRow = FilemetadataRow(uuidSource.uuid, _, _, now, userId, _)

    val rowsWithMatchId: List[Rows] = ((allEmptyDirectoryNodes ++ allFileNodes) map {
      case (path, treeNode) =>
        val parentId = treeNode.parentPath.map(path => allFileNodes.getOrElse(path, allEmptyDirectoryNodes(path)).id)
        val fileId = treeNode.id
        val fileRow = FileRow(fileId, consignmentId, userId, now, filetype = Some(treeNode.treeNodeType), filename = Some(treeNode.name), parentid = parentId)
        val commonMetadataRows = row(fileId, path, ClientSideOriginalFilepath) ::
          staticMetadataProperties.map(property => row(fileId, property.value, property.name))
        if (treeNode.treeNodeType.isFileType) {
          val input = addFileAndMetadataInput.metadataInput.filter(m => {
            val pathWithoutSlash = if (m.originalPath.startsWith("/")) m.originalPath.tail else m.originalPath
            pathWithoutSlash == path
          }).head
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

    for {
      _ <- rowsWithMatchId.grouped(batchSize).map(row => fileRepository.addFiles(row.map(_.fileRow), row.flatMap(_.metadataRows))).toList.head
    } yield rowsWithMatchId.flatMap {
      case MatchedFileRows(fileRow, _, matchId) => FileMatches(fileRow.fileid, matchId) :: Nil
      case _ => Nil
    }
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    consignmentRepository.getConsignmentsOfFiles(fileIds)
      .map(_.map(consignmentByFile => FileOwnership(consignmentByFile._1, consignmentByFile._2.userid)))
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  def getFileMetadata(consignmentId: UUID, fileFilters: Option[FileFilters] = None): Future[List[File]] = {
    val filters = fileFilters.getOrElse(FileFilters())
    for {
      fileAndMetadataList <- fileRepository.getFiles(consignmentId, filters)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId)
      ffidStatus <- fileStatusService.getFileStatus(consignmentId)
    } yield fileAndMetadataList.toFiles(avList, ffidMetadataList, ffidStatus).toList
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

    def toFiles(avMetadata: List[AntivirusMetadata], ffidMetadata: List[FFIDMetadata], ffidStatus: Map[UUID, String]): Seq[File] = {
      response.groupBy(_._1).map {
        case (fr, fmr) =>
          val fileId = fr.fileid
          File(
            fileId, fr.filetype, fr.filename, fr.parentid,
            convertMetadataRows(fmr.flatMap(_._2)), ffidStatus.get(fileId), ffidMetadata.find(_.fileId == fileId), avMetadata.find(_.fileId == fileId))
      }.toSeq
    }
  }

  trait Rows {
    val fileRow: FileRow
    val metadataRows: List[FilemetadataRow]
  }

  case class MatchedFileRows(fileRow: FileRow, metadataRows: List[FilemetadataRow], matchId: Long) extends Rows

  case class DirectoryRows(fileRow: FileRow, metadataRows: List[FilemetadataRow]) extends Rows

  case class FileOwnership(fileId: UUID, userId: UUID)
}
