package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.model.Statuses._

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository, uuidSource: UUIDSource)(implicit
    val executionContext: ExecutionContext
) {

  private def toFileStatuses(rows: Seq[FilestatusRow]): Seq[FileStatus] = {
    rows.map(r => FileStatus(r.fileid, r.statustype, r.value))
  }

  def addFileStatuses(addMultipleFileStatusesInput: AddMultipleFileStatusesInput): Future[List[FileStatus]] = {
    val rows = addMultipleFileStatusesInput.statuses.map(addFileStatusInput => {
      FilestatusRow(uuidSource.uuid, addFileStatusInput.fileId, addFileStatusInput.statusType, addFileStatusInput.statusValue, Timestamp.from(Instant.now()))
    })
    fileStatusRepository.addFileStatuses(rows).map(_.map(row => FileStatus(row.fileid, row.statustype, row.value)).toList)
  }

  @deprecated("Use addFileStatuses(addMultipleFileStatuses: List[AddFileStatusInput])")
  def addFileStatus(addFileStatusInput: AddFileStatusInput): Future[FileStatus] = {
    addFileStatuses(AddMultipleFileStatusesInput(addFileStatusInput :: Nil)).map(_.head)
  }

  def getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None): Future[List[FileStatus]] = {
    for {
      rows <- fileStatusRepository.getFileStatus(consignmentId, statusTypes, selectedFileIds)
    } yield toFileStatuses(rows).toList
  }

  @deprecated("Use getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None)")
  def getFileStatus(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, String]] = {
    for {
      ffidStatus <- getFileStatuses(consignmentId, Set(FFIDType.id), selectedFileIds)
      fileStatusMap = ffidStatus.flatMap(row => Map(row.fileId -> row.statusValue)).toMap
    } yield fileStatusMap
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    val statusTypes = Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id)
    fileStatusRepository
      .getFileStatus(consignmentId, statusTypes)
      .map(fileChecks => {
        !fileChecks.map(_.value).exists(_ != SuccessValue.value) &&
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id).forall(fileChecks.map(_.statustype).toSet.contains)
      })
  }
}

object FileStatusService {
  val allFileStatusTypes: Set[StatusType] = Set(
    ChecksumMatchType,
    AntivirusType,
    FFIDType,
    RedactionType,
    UploadType,
    ServerChecksumType,
    ClientChecksType,
    ClosureMetadataType,
    DescriptiveMetadataType
  )

  val defaultStatuses: Map[StatusType, StatusValue] = Map(ClosureMetadataType -> NotEnteredValue, DescriptiveMetadataType -> NotEnteredValue)
}
