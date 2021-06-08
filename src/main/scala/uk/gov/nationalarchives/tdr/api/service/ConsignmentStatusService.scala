package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus

import java.sql.Timestamp
import java.util.UUID
import java.time.Instant.now
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(consignmentStatusRepository: ConsignmentStatusRepository,
                               timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {

  def getConsignmentStatus(consignmentId: UUID): Future[CurrentStatus] = {
    for {
      upload <- consignmentStatusRepository.getConsignmentStatus(consignmentId)
    } yield CurrentStatus(upload.sortBy(t => t.createddatetime).reverse.map(_.value).headOption)
  }

  def setUploadConsignmentStatusValueToComplete(consignmentId: UUID): Future[Int] = {
    consignmentStatusRepository.setUploadConsignmentStatusValueToComplete(
      consignmentId,
      "Upload",
      "Completed",
      Timestamp.from(timeSource.now)
    )
  }
}
