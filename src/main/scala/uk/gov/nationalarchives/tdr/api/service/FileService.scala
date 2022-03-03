package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, fileTypeIdentifier, folderTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileService(
                   fileRepository: FileRepository,
                   consignmentRepository: ConsignmentRepository,
                   consignmentStatusRepository: ConsignmentStatusRepository,
                   fileMetadataService: FileMetadataService,
                   ffidMetadataService: FFIDMetadataService,
                   avMetadataService: AntivirusMetadataService,
                   timeSource: TimeSource,
                   uuidSource: UUIDSource
                 )(implicit val executionContext: ExecutionContext) {

  implicit class FileRowHelper(fr: Option[FileRow]) {
    def fileType: Option[String] = fr.flatMap(_.filetype)

    def fileName: Option[String] = fr.flatMap(_.filename)

    def parentId: Option[UUID] = fr.flatMap(_.parentid)
  }

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource)

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, userId: UUID): Future[List[FileMatches]] = {
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val filePaths = addFileAndMetadataInput.metadataInput.map(_.originalPath).toSet
    val allFileNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(filePaths, fileTypeIdentifier)
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(addFileAndMetadataInput.emptyDirectories.toSet, folderTypeIdentifier)

    val folderIdTopath: Map[UUID, String] = (allEmptyDirectoryNodes ++ allFileNodes.filter(_._2.treeNodeType == folderTypeIdentifier))
      .map(f => f._2.id -> f._1)

    val fileRows: List[FileRow] = ((allEmptyDirectoryNodes ++ allFileNodes) map {
      case (_, treeNode) =>
        val parentId = treeNode.parentPath.map(v => allFileNodes.getOrElse(v, allEmptyDirectoryNodes(v)).id)
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
        }) ++ fileRows.filter(_.filetype.get.isFolderType).flatMap(folder => {
          row(folder.fileid, folderIdTopath(folder.fileid), ClientSideOriginalFilepath) ::
            staticMetadataProperties.map(property => row(folder.fileid, property.value, property.name))
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

  def getFileMetadata(consignmentId: UUID, fileFilters: Option[FileFilters] = None): Future[List[File]] = {
    for {
      fileList <- fileRepository.getFiles(consignmentId, FileFilters(None))
      fileMetadataList <- fileMetadataService.getFileMetadata(consignmentId)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId)
    } yield {
      fileMetadataList map {
        case (fileId, fileMetadata) =>
          val fr: Option[FileRow] = fileList.find(_.fileid == fileId)
          File(fileId,
            fr.fileType, fr.fileName, fr.parentId, fileMetadata, ffidMetadataList.find(_.fileId == fileId), avList.find(_.fileId == fileId))
      }
    }.toList
  }
}

case class FileOwnership(fileId: UUID, userId: UUID)
