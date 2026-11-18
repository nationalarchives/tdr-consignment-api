package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit
    val executionContext: ExecutionContext
) {

  private def toFileStatuses(rows: Seq[FilestatusRow]): Seq[FileStatus] = {
    rows.map(r => FileStatus(r.fileid, r.statustype, r.value))
  }

  def addFileStatuses(addMultipleFileStatusesInput: AddMultipleFileStatusesInput): Future[List[FileStatus]] = {
    fileStatusRepository.addFileStatuses(addMultipleFileStatusesInput.statuses).map(_.map(row => FileStatus(row.fileid, row.statustype, row.value)).toList)
  }

  def getConsignmentFileProgress(consignmentId: UUID): Future[FileChecks] = {
    fileStatusRepository
      .countFileStatusByType(consignmentId, Set(FFID, ChecksumMatch, Antivirus))
      .map(counts => {
        val countByStatusType = counts.toMap
        FileChecks(
          AntivirusProgress(countByStatusType.getOrElse(Antivirus, 0)),
          ChecksumProgress(countByStatusType.getOrElse(ChecksumMatch, 0)),
          FFIDProgress(countByStatusType.getOrElse(FFID, 0))
        )
      })
  }

  def getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None): Future[List[FileStatus]] = {
    for {
      rows <- fileStatusRepository.getFileStatus(consignmentId, statusTypes, selectedFileIds)
    } yield toFileStatuses(rows).toList
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    val statusTypes = Set(ChecksumMatch, Antivirus, FFID, Redaction)
    val requiredStatusTypes = Set(ChecksumMatch, Antivirus, FFID)
    fileStatusRepository
      .getDistinctFileStatusValues(consignmentId, statusTypes)
      .map(statuses => {
        !statuses.exists { case (_, value) => value != Success } &&
        requiredStatusTypes.subsetOf(statuses.map { case (statusType, _) => statusType }.toSet)
      })
  }
}

object FileStatusService {
  // Status types
  val ChecksumMatch = "ChecksumMatch"
  val Antivirus = "Antivirus"
  val FFID = "FFID"
  val Redaction = "Redaction"
  val Upload = "Upload"
  val ServerChecksum = "ServerChecksum"
  val ClientChecks = "ClientChecks"
  val ClientFilePath = "ClientFilePath"

  val allFileStatusTypes: Set[String] = Set(ChecksumMatch, Antivirus, FFID, Redaction, Upload, ServerChecksum, ClientChecks)

  // Values
  val Success = "Success"
  val Failed = "Failed"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
  val PasswordProtected = "PasswordProtected"
  val Zip = "Zip"
  val NonJudgmentFormat = "NonJudgmentFormat"
  val ZeroByteFile = "ZeroByteFile"
  val InProgress = "InProgress"
  val Completed = "Completed"
}
