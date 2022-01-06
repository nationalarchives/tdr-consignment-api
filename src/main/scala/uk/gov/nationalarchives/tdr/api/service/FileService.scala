package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.util.UUID

import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput, FileMatches, TreeNodeType}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.FileTypeHelper
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils._

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

  private val treeNodesUtils: TreeNodesUtils = TreeNodesUtils(uuidSource)

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
          File(fileId, fileMetadata, ffidMetadataList.find(_.fileId == fileId), avList.find(_.fileId == fileId))
      }
    }.toList
  }

  //function to update all file metadata when user has chosen to update an entire folder
  //'customMetadataInputRepresentation' is a mock of what the input could be
  def updateAllDescendantsMetadata(fileId: UUID, customMetadataInputRepresentation: Map[String, String], userId: UUID): Unit = {
    for {
      allDescendents <- fileRepository.getAllDescendentsCTE(fileId)
      ids = allDescendents.filter(_.filetype.get.isFileType).map(_.fileid)
      rows = ids.map(id => {
        customMetadataInputRepresentation.map(md => {
          FilemetadataRow(uuidSource.uuid, id, md._2, Timestamp.from(timeSource.now), userId, md._1)
        })
      })
    } yield fileRepository.updateFileMetadata(rows.flatten.toList)
  }

  //would need to retrieve the top level node using consignment id
  //because on initial UI page for custom metadata would only have the consignment id available
  def getTopLevel(consignmentId: UUID): Future[List[TreeNode]] = {
    for {
      rows <- fileRepository.getTopLevelFolder(consignmentId)
    } yield rows.map(r => TreeNode(r.fileid, r.filename.get, None, r.filetype.get)).toList
  }

  //function to retrieve all the direct children of the given node
  //would support the custom metadata UI
  //only need direct children of the file for the UI
  def getDirectDescendents(fileId: UUID): Future[List[TreeNode]] = {
    for {
      rows <- fileRepository.getDirectDescendents(fileId)
    } yield rows.map(r => TreeNode(r.fileid, r.filename.get, None, r.filetype.get)).toList
  }

  //temp function to test the 'CTE' SQL. Wouldn't be needed for the application
  def getAllDescendentsCTE(fileId: UUID, propertyNames: List[String] = List()): Future[List[TreeNode]] = {
    for {
      rows <- fileRepository.getAllDescendentsCTE(fileId)
    } yield rows.map(r => TreeNode(r.fileid, r.filename.get, None, r.filetype.get)).toList
  }
}

case class FileOwnership(fileId: UUID, userId: UUID)
case class MetadataProperties(propertyName: String, propertyValue: String)
