package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{ConsignmentstatusRow, FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, AddFilesInput, FileMatches, Files, ClientSideMetadataInput}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.LongUtils

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

  def addFile(addFilesInput: AddFilesInput, userId: UUID): Future[Files] = {
    val now = Timestamp.from(timeSource.now)

    val rows: Seq[FileRow] = List.fill(addFilesInput.numberOfFiles)(1)
      .map(_ => FileRow(uuidSource.uuid, addFilesInput.consignmentId, userId, now))

    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, addFilesInput.consignmentId, "Upload", "InProgress", now)

    for {
      _ <- consignmentRepository.addParentFolder(addFilesInput.consignmentId, addFilesInput.parentFolder)
      files <- fileRepository.addFiles(rows, consignmentStatusRow)
      _ <- fileMetadataService.addStaticMetadata(files, userId)
    } yield Files(files.map(_.fileid))
  }

  def addFile(addFileAndMetadataInput: AddFileAndMetadataInput, userId: UUID): Future[List[FileMatches]] = {
    val now = Timestamp.from(timeSource.now)
    val consignmentId = addFileAndMetadataInput.consignmentId
    val fileRows: List[FileRow] = List.fill(addFileAndMetadataInput.metadataInput.size)(1)
      .map(_ => {
        FileRow(uuidSource.uuid, consignmentId, userId, now)
      })
    val metadataWithIds: List[(UUID, ClientSideMetadataInput)] = fileRows.map(_.fileid).zip(addFileAndMetadataInput.metadataInput)
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
      _ <- if (addFileAndMetadataInput.isComplete) {
        consignmentStatusRepository.updateConsignmentStatus(consignmentId, "Upload", "Completed", now)
      } else {
        Future.successful(())
      }

    } yield metadataWithIds.map(m => (m._1, m._2.matchId)).map(f => FileMatches(f._1, f._2))
  }

  def getOwnersOfFiles(fileIds: Seq[UUID]): Future[Seq[FileOwnership]] = {
    consignmentRepository.getConsignmentsOfFiles(fileIds)
      .map(_.map(consignmentByFile => FileOwnership(consignmentByFile._1, consignmentByFile._2.userid)))
  }

  def fileCount(consignmentId: UUID): Future[Int] = {
    fileRepository.countFilesInConsignment(consignmentId)
  }

  def getFiles(consignmentId: UUID): Future[Files] = {
    fileRepository.getFilesWithPassedAntivirus(consignmentId).map(r => Files(r.map(_.fileid)))
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
}

case class FileOwnership(fileId: UUID, userId: UUID)
