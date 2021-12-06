package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Checksum, FFID, Success}
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit val executionContext: ExecutionContext) {

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    for {
      checksumStatus <- fileStatusRepository.getFileStatus(consignmentId, Checksum)
      avStatus <- fileStatusRepository.getFileStatus(consignmentId, Antivirus)
      ffidStatus <- fileStatusRepository.getFileStatus(consignmentId, FFID)
    } yield {
      checksumStatus.nonEmpty && avStatus.nonEmpty && ffidStatus.nonEmpty &&
        (checksumStatus.filter(_.value != Success) ++ avStatus.filter(_.value != Success) ++ ffidStatus.filter(_.value != Success)).isEmpty
    }
  }
}

object FileStatusService {
  //Status types
  val Checksum = "Checksum"
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
