package uk.gov.nationalarchives.tdr.api.service

import cats.implicits._
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentStatusRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.{ConsignmentStatus, ConsignmentStatusInput}
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(
    consignmentStatusRepository: ConsignmentStatusRepository,
    fileStatusRepository: FileStatusRepository,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  implicit class ConsignmentStatusInputHelper(input: ConsignmentStatusInput) {
    def statusValueToString: String = input.statusValue.getOrElse("")
  }

  private def toConsignmentStatus(row: ConsignmentstatusRow): ConsignmentStatus = {
    ConsignmentStatus(
      row.consignmentstatusid,
      row.consignmentid,
      row.statustype,
      row.value,
      row.createddatetime.toZonedDateTime,
      row.modifieddatetime.map(timestamp => timestamp.toZonedDateTime)
    )
  }

  def addConsignmentStatus(addConsignmentStatusInput: ConsignmentStatusInput): Future[ConsignmentStatus] = {
    validateStatusTypeAndValue(addConsignmentStatusInput)

    for {
      consignmentStatuses <- consignmentStatusRepository.getConsignmentStatus(addConsignmentStatusInput.consignmentId)
      statusType = addConsignmentStatusInput.statusType
      existingConsignmentStatusRow = consignmentStatuses.find(csr => csr.statustype == statusType)
      consignmentStatusRow <- {
        if (existingConsignmentStatusRow.isDefined) {
          throw ConsignmentStateException(
            s"Existing consignment $statusType status is '${existingConsignmentStatusRow.get.value}'; new entry cannot be added"
          )
        }
        val consignmentStatusRow: ConsignmentstatusRow = ConsignmentstatusRow(
          uuidSource.uuid,
          addConsignmentStatusInput.consignmentId,
          addConsignmentStatusInput.statusType,
          addConsignmentStatusInput.statusValueToString,
          Timestamp.from(timeSource.now)
        )
        consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
      }
    } yield {
      toConsignmentStatus(consignmentStatusRow)
    }
  }

  def getConsignmentStatuses(consignmentId: UUID): Future[List[ConsignmentStatus]] = {
    for {
      rows <- consignmentStatusRepository.getConsignmentStatus(consignmentId)
    } yield {
      rows.map(r => toConsignmentStatus(r)).toList
    }
  }

  def updateConsignmentStatus(updateConsignmentStatusInput: ConsignmentStatusInput): Future[Int] = {
    validateStatusTypeAndValue(updateConsignmentStatusInput)
    consignmentStatusRepository.updateConsignmentStatus(
      updateConsignmentStatusInput.consignmentId,
      updateConsignmentStatusInput.statusType,
      updateConsignmentStatusInput.statusValue.get,
      Timestamp.from(timeSource.now)
    )
  }

  def updateMetadataConsignmentStatus(consignmentId: UUID, statusTypes: List[String]): Future[List[Int]] = {
    fileStatusRepository
      .getFileStatus(consignmentId, statusTypes.toSet)
      .flatMap(fileStatuses => {
        statusTypes
          .map(statusType => {
            val fileStatusesForType = fileStatuses.filter(_.statustype == statusType)
            val statusValue = if (fileStatusesForType.count(_.value == NotEntered) == fileStatusesForType.length) {
              NotEntered
            } else if (fileStatusesForType.exists(_.value == Incomplete)) {
              Incomplete
            } else {
              Completed
            }
            val input = ConsignmentStatusInput(consignmentId, statusType, Option(statusValue))
            updateConsignmentStatus(input)
          })
          .sequence
      })
  }

  private def validateStatusTypeAndValue(consignmentStatusInput: ConsignmentStatusInput): Boolean = {
    val statusType: String = consignmentStatusInput.statusType
    val statusValue: String = consignmentStatusInput.statusValueToString

    if (validStatusTypes.contains(statusType) && validStatusValues.contains(statusValue)) {
      true
    } else {
      throw InputDataException(s"Invalid ConsignmentStatus input: either '$statusType' or '$statusValue'")
    }
  }
}

object ConsignmentStatusService {
  val validConsignmentTypes: List[String] =
    List("Series", "TransferAgreement", "Upload", "ClientChecks", "DraftMetadata", "ClosureMetadata", "DescriptiveMetadata", "ConfirmTransfer", "Export")
  val validStatusTypes: Set[String] = validConsignmentTypes.toSet ++ Set("ServerFFID", "ServerChecksum", "ServerAntivirus")
  val validStatusValues: Set[String] = Set("InProgress", "Completed", "CompletedWithIssues", "Failed", "NotEntered", "Incomplete")
}
