package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.{DisallowedPuidsRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, ChecksumMatch, FFID, Success}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(
                         fileStatusRepository: FileStatusRepository,
                         disallowedPuidsRepository: DisallowedPuidsRepository)(implicit val executionContext: ExecutionContext) {

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

  //Values
  val Success = "Success"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
  val PasswordProtected = "PasswordProtected"
  val Zip = "Zip"
  val NonJudgmentFormat = "NonJudgmentFormat"
  val ZeroByteFile = "ZeroByteFile"
}
