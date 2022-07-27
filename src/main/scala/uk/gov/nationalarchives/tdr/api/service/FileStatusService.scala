package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, ChecksumMatch, FFID, Success}
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit val executionContext: ExecutionContext) {

  def getFileStatus(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, String]] = {
    for {
      ffidStatus <- fileStatusRepository.getFileStatus(consignmentId, FFID, selectedFileIds)
      fileStatusMap = ffidStatus.flatMap(fileStatusRow => Map(fileStatusRow.fileid -> fileStatusRow.value)).toMap
    } yield fileStatusMap
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    val getChecksumMatchStatus = fileStatusRepository.getFileStatus(consignmentId, ChecksumMatch)
    val getAvStatus = fileStatusRepository.getFileStatus(consignmentId, Antivirus)
    val getFfidStatus = fileStatusRepository.getFileStatus(consignmentId, FFID)
    for {
      checksumMatchStatus <- getChecksumMatchStatus
      avStatus <- getAvStatus
      ffidStatus <- getFfidStatus
    } yield {
      checksumMatchStatus.nonEmpty && avStatus.nonEmpty && ffidStatus.nonEmpty &&
        (checksumMatchStatus.filter(_.value != Success) ++ avStatus.filter(_.value != Success) ++ ffidStatus.filter(_.value != Success)).isEmpty
    }
  }
}

object FileStatusService {
  //Status types
  val ChecksumMatch = "ChecksumMatch"
  val Antivirus = "Antivirus"
  val FFID = "FFID"

  //Values
  val Success = "Success"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
  val PasswordProtected = "PasswordProtected"
  val Zip = "Zip"
  val NonJudgmentFormat = "NonJudgmentFormat"
}
