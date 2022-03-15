package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.util.UUID

import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.FileRepositoryMetadata
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{FileTypeHelper, directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
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
                   uuidSource: UUIDSource
                 )(implicit val executionContext: ExecutionContext) {

  implicit class FileRepositoryResponseHelper(response: Seq[FileRepositoryMetadata]) {
    private def convertMetadataRows(rows: Seq[FilemetadataRow]): FileMetadataValues = {
      val propertyNameMap = rows.groupBy(_.propertyname).transform((_, value) => value.head.value)
      FileMetadataValues(
        propertyNameMap.get(SHA256ClientSideChecksum),
        propertyNameMap.get(ClientSideOriginalFilepath),
        propertyNameMap.get(ClientSideFileLastModifiedDate).map(d => Timestamp.valueOf(d).toLocalDateTime),
        propertyNameMap.get(ClientSideFileSize).map(_.toLong),
        propertyNameMap.get(RightsCopyright.name),
        propertyNameMap.get(LegalStatus.name),
        propertyNameMap.get(HeldBy.name),
        propertyNameMap.get(Language.name),
        propertyNameMap.get(FoiExemptionCode.name)
      )
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
    val allEmptyDirectoryNodes: Map[String, TreeNode] = treeNodesUtils.generateNodes(addFileAndMetadataInput.emptyDirectories.toSet, directoryTypeIdentifier)

    val folderIdToPath: Map[UUID, String] = (allEmptyDirectoryNodes ++ allFileNodes.filter(_._2.treeNodeType == directoryTypeIdentifier))
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
        })
      case _ => Seq()
    } ++ fileRows.filter(_.filetype.get.isDirectoryType).flatMap(directory => {
      row(directory.fileid, folderIdToPath(directory.fileid), ClientSideOriginalFilepath) ::
        staticMetadataProperties.map(property => row(directory.fileid, property.value, property.name))
    })
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
    val filters = fileFilters.getOrElse(FileFilters())
    for {
      fileAndMetadataList <- fileRepository.getFiles(consignmentId, filters)
      ffidMetadataList <- ffidMetadataService.getFFIDMetadata(consignmentId)
      avList <- avMetadataService.getAntivirusMetadata(consignmentId)
      ffidStatus <- fileStatusService.getFileStatus(consignmentId)
    } yield fileAndMetadataList.toFiles(avList, ffidMetadataList, ffidStatus).toList
  }
}

case class FileOwnership(fileId: UUID, userId: UUID)
