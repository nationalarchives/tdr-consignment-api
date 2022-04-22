package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.UpdateConsignmentStatusInput
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}

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
      CurrentStatus(consignmentStatusTypesAndVals.get("Series"),
        consignmentStatusTypesAndVals.get("TransferAgreement"),
        consignmentStatusTypesAndVals.get("Upload"),
        consignmentStatusTypesAndVals.get("ConfirmTransfer"),
        consignmentStatusTypesAndVals.get("Export")
      )
    }
  }

  def updateConsignmentStatus(consignmentId: UUID, statusType: String, statusValue: String): Future[Int] = {
    consignmentStatusRepository.updateConsignmentStatus(
      consignmentId,
      statusType,
      statusValue,
      Timestamp.from(timeSource.now)
    )
  }

  def updateConsignmentStatus(updateConsignmentStatusInput: UpdateConsignmentStatusInput): Future[Int] = {
    val statusType: String = updateConsignmentStatusInput.statusType
    val statusValue: String = updateConsignmentStatusInput.statusValue

    if(validStatusTypes.contains(statusType) && validStatusValues.contains(statusValue)) {
      consignmentStatusRepository.updateConsignmentStatus(
        updateConsignmentStatusInput.consignmentId,
        updateConsignmentStatusInput.statusType,
        updateConsignmentStatusInput.statusValue,
        Timestamp.from(timeSource.now)
      )
    } else {
      throw InputDataException(s"Invalid updateConsignmentStatus input: either '$statusType' or '$statusValue'")
    }
  }
}

object ConsignmentStatusService {
  val validStatusTypes = Set("Series", "TransferAgreement", "Upload", "ConfirmTransfer", "Export")
  val validStatusValues = Set("InProgress", "Completed")
}
