package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.{ConsignmentStatus, ConsignmentStatusInput}
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(consignmentStatusRepository: ConsignmentStatusRepository,
                               uuidSource: UUIDSource,
                               timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {

  def addConsignmentStatus(addConsignmentStatusInput: ConsignmentStatusInput): Future[ConsignmentStatus] = {
    validateStatusTypeAndValue(addConsignmentStatusInput)

    for {
      consignmentStatuses <- consignmentStatusRepository.getConsignmentStatus(addConsignmentStatusInput.consignmentId)
      statusType = addConsignmentStatusInput.statusType
      existingConsignmentStatusRow = consignmentStatuses.find(csr => csr.statustype == statusType)
      consignmentStatusRow <- {
        if(existingConsignmentStatusRow.isDefined) {
          throw ConsignmentStateException(
            s"Existing consignment $statusType status is '${existingConsignmentStatusRow.get.value}'; new entry cannot be added"
          )
        }
        val consignmentStatusRow: ConsignmentstatusRow = ConsignmentstatusRow(
          uuidSource.uuid,
          addConsignmentStatusInput.consignmentId,
          addConsignmentStatusInput.statusType,
          addConsignmentStatusInput.statusValue,
          Timestamp.from(timeSource.now)
        )
        consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
      }
    } yield {
        ConsignmentStatus(
          consignmentStatusRow.consignmentstatusid,
          consignmentStatusRow.consignmentid,
          consignmentStatusRow.statustype,
          consignmentStatusRow.value,
          consignmentStatusRow.createddatetime.toZonedDateTime,
          consignmentStatusRow.modifieddatetime.map(timestamp => timestamp.toZonedDateTime)
        )
      }
    }

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

  def updateConsignmentStatus(updateConsignmentStatusInput: ConsignmentStatusInput): Future[Int] = {
    validateStatusTypeAndValue(updateConsignmentStatusInput)
    consignmentStatusRepository.updateConsignmentStatus(
      updateConsignmentStatusInput.consignmentId,
      updateConsignmentStatusInput.statusType,
      updateConsignmentStatusInput.statusValue,
      Timestamp.from(timeSource.now)
    )
  }

  private def validateStatusTypeAndValue(consignmentStatusInput: ConsignmentStatusInput) = {
    val statusType: String = consignmentStatusInput.statusType
    val statusValue: String = consignmentStatusInput.statusValue

    if(validStatusTypes.contains(statusType) && validStatusValues.contains(statusValue)) {
      true
    } else {
      throw InputDataException(s"Invalid ConsignmentStatus input: either '$statusType' or '$statusValue'")
    }
  }
}

object ConsignmentStatusService {
  val validStatusTypes = Set("Series", "TransferAgreement", "Upload", "ConfirmTransfer", "Export")
  val validStatusValues = Set("InProgress", "Completed", "Failed")
}
