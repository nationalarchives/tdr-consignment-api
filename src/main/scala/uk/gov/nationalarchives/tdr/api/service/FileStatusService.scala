package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.Success

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit val executionContext: ExecutionContext) {

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    for {
      checksumStatus <- fileStatusRepository.getFileStatus(consignmentId, "checksum")
    } yield checksumStatus.headOption.exists(_.value == Success)
  }

}

object FileStatusService {
  //Status types
  val Checksum = "checksum"

  //Values
  val Success = "Success"
  val Mismatch = "Mismatch"
}
