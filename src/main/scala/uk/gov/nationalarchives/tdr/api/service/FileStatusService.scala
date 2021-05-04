package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Checksum, Success}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit val executionContext: ExecutionContext) {

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    for {
      checksumStatus <- fileStatusRepository.getFileStatus(consignmentId, Checksum)
      avStatus <- fileStatusRepository.getFileStatus(consignmentId, Antivirus)
    } yield
      checksumStatus.headOption.exists(_.value == Success) &&
        avStatus.headOption.exists(_.value == Success)
  }

}

object FileStatusService {
  //Status types
  val Checksum = "Checksum"
  val Antivirus = "Antivirus"

  //Values
  val Success = "Success"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
}
