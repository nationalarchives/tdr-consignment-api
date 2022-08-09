package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{DisallowedPuidsRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, ChecksumMatch, FFID, Failed, Success, Upload}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(
                         fileStatusRepository: FileStatusRepository,
                         disallowedPuidsRepository: DisallowedPuidsRepository,
                         uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {

  def addFileStatus(addFileStatusInput: AddFileStatusInput): Future[FileStatus] = {

    validateStatusTypeAndValue(addFileStatusInput)
    for {
      _ <- fileStatusRepository.addFileStatuses(List(FilestatusRow(uuidSource.uuid, addFileStatusInput.fileId, addFileStatusInput.statusType,
        addFileStatusInput.statusValue, Timestamp.from(Instant.now()))))
    } yield FileStatus(addFileStatusInput.fileId, addFileStatusInput.statusType, addFileStatusInput.statusValue)
  }

  private def validateStatusTypeAndValue(addFileStatusInput: AddFileStatusInput) = {
    val statusType: String = addFileStatusInput.statusType
    val statusValue: String = addFileStatusInput.statusValue

    if (Upload == statusType && List(Success, Failed).contains(statusValue)) {
      true
    } else {
      throw InputDataException(s"Invalid FileStatus input: either '$statusType' or '$statusValue'")
    }
  }

  def getFileStatus(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, String]] = {
    for {
      ffidStatus <- fileStatusRepository.getFileStatus(consignmentId, FFID, selectedFileIds)
      fileStatusMap = ffidStatus.flatMap(fileStatusRow => Map(fileStatusRow.fileid -> fileStatusRow.value)).toMap
    } yield fileStatusMap
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    for {
      checksumMatchStatus <- fileStatusRepository.getFileStatus(consignmentId, ChecksumMatch)
      avStatus <- fileStatusRepository.getFileStatus(consignmentId, Antivirus)
      ffidStatusRows <- fileStatusRepository.getFileStatus(consignmentId, FFID)
      ffidStatuses = ffidStatusRows.map(_.value)
      failedFFIDStatuses <- disallowedPuidsRepository.activeReasons()
    } yield {
      checksumMatchStatus.nonEmpty && avStatus.nonEmpty && ffidStatuses.nonEmpty &&
        (checksumMatchStatus.filter(_.value != Success) ++ avStatus.filter(_.value != Success) ++ ffidStatuses.filter(failedFFIDStatuses.contains(_))).isEmpty
    }
  }
}

object FileStatusService {
  //Status types
  val ChecksumMatch = "ChecksumMatch"
  val Antivirus = "Antivirus"
  val FFID = "FFID"
  val Upload = "Upload"

  //Values
  val Success = "Success"
  val Failed = "Failed"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
  val PasswordProtected = "PasswordProtected"
  val Zip = "Zip"
  val NonJudgmentFormat = "NonJudgmentFormat"
  val ZeroByteFile = "ZeroByteFile"
}
