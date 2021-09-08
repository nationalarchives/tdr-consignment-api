package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(consignmentStatusRepository: ConsignmentStatusRepository,
                               timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {

  def getConsignmentStatus(consignmentId: UUID): Future[CurrentStatus] = {
    for {
      consignmentStatuses <- consignmentStatusRepository.getConsignmentStatus(consignmentId)
    } yield {
      val consignmentStatusTypesAndVals = consignmentStatuses.map(cs => (cs.statustype, cs.value)).toMap
      CurrentStatus(consignmentStatusTypesAndVals.get("TransferAgreement"), consignmentStatusTypesAndVals.get("Upload"))
    }
  }

  def setUploadConsignmentStatusValueToComplete(consignmentId: UUID): Future[Int] = {
    consignmentStatusRepository.updateConsignmentStatus(
      consignmentId,
      "Upload",
      "Completed",
      Timestamp.from(timeSource.now)
    )
  }
}
