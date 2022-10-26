package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentStatusRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.{ConsignmentStatus, ConsignmentStatusInput}
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.Success
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(consignmentStatusRepository: ConsignmentStatusRepository,
                               fileStatusRepository: FileStatusRepository,
                               uuidSource: UUIDSource,
                               timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {
  private val statusesToUpdateBasedOnFile: Set[String] = Set("Upload")

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
          addConsignmentStatusInput.statusValue.getOrElse(""),
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
    val statusType: String = updateConsignmentStatusInput.statusType

    if (statusesToUpdateBasedOnFile.contains(statusType)) {
      updateBasedOnFileStatus(updateConsignmentStatusInput.consignmentId, statusType)
    } else {
      validateStatusTypeAndValue(updateConsignmentStatusInput)
      consignmentStatusRepository.updateConsignmentStatus(
        updateConsignmentStatusInput.consignmentId,
        updateConsignmentStatusInput.statusType,
        updateConsignmentStatusInput.statusValue.get, // if this value is None, then it should through an error as this should not be possible after the validation
        Timestamp.from(timeSource.now)
      )
    }
  }

  private def updateBasedOnFileStatus(consignmentId: UUID, statusType: String): Future[Int] = {
    val noFileStatusesError = s"Error: There are no $statusType statuses for any of the files from consignment $consignmentId"
    for {
      fileUploadStatuses <- fileStatusRepository.getFileStatus(consignmentId, statusType)
      successful = if(fileUploadStatuses.isEmpty) throw InputDataException(noFileStatusesError) else fileUploadStatuses.forall(_.value == Success)
      consignmentStatus = if(successful) "Completed" else "CompletedWithIssues"
      updated <- consignmentStatusRepository.updateConsignmentStatus(consignmentId, statusType, consignmentStatus, Timestamp.from(timeSource.now))
    } yield updated
  }

  private def validateStatusTypeAndValue(consignmentStatusInput: ConsignmentStatusInput): Boolean = {
    val statusType: String = consignmentStatusInput.statusType
    val statusValue: String = consignmentStatusInput.statusValue.getOrElse("")

    if(validStatusTypes.contains(statusType) && validStatusValues.contains(statusValue)) {
      true
    } else {
      throw InputDataException(s"Invalid ConsignmentStatus input: either '$statusType' or '$statusValue'")
    }
  }
}

object ConsignmentStatusService {
  val validStatusTypes: Set[String] = Set("Series", "TransferAgreement", "Upload", "ConfirmTransfer", "Export", "ClientChecks")
  val validStatusValues: Set[String] = Set("InProgress", "Completed", "CompletedWithIssues", "Failed")
}
