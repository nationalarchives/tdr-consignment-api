package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{ConsignmentstatusRow, MetadatareviewlogRow}
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentStatusRepository, MetadataReviewLogRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.{ConsignmentStatus, ConsignmentStatusInput}
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}
import uk.gov.nationalarchives.tdr.api.utils.Statuses.{CompletedValue, CompletedWithIssuesValue, FailedValue, InProgressValue, MetadataReviewType}
import uk.gov.nationalarchives.tdr.api.utils.{Approval, Rejection, Submission}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusService(
    consignmentStatusRepository: ConsignmentStatusRepository,
    metadataReviewLogRepository: MetadataReviewLogRepository,
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

  def addConsignmentStatus(addConsignmentStatusInput: ConsignmentStatusInput, userId: UUID): Future[ConsignmentStatus] = {
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
      _ <- metadataReviewLogEntry(addConsignmentStatusInput, userId)
        .map(metadataReviewLogRepository.addLogEntry)
        .getOrElse(Future.unit)
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

  def updateConsignmentStatus(updateConsignmentStatusInput: ConsignmentStatusInput, userId: UUID): Future[Int] = {
    validateStatusTypeAndValue(updateConsignmentStatusInput)
    for {
      rows <- consignmentStatusRepository.updateConsignmentStatus(
        updateConsignmentStatusInput.consignmentId,
        updateConsignmentStatusInput.statusType,
        updateConsignmentStatusInput.statusValue.get,
        Timestamp.from(timeSource.now)
      )
      _ <- metadataReviewLogEntry(updateConsignmentStatusInput, userId)
        .map(metadataReviewLogRepository.addLogEntry)
        .getOrElse(Future.unit)
    } yield rows
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

  private def metadataReviewLogEntry(consignmentStatusInput: ConsignmentStatusInput, userId: UUID): Option[MetadatareviewlogRow] = {
    val action = Option
      .when(consignmentStatusInput.statusType == MetadataReviewType.id) {
        consignmentStatusInput.statusValue.map {
          case InProgressValue.value          => Submission.value
          case CompletedValue.value          => Approval.value
          case CompletedWithIssuesValue.value => Rejection.value
        }
      }
      .flatten
    action.map(a => MetadatareviewlogRow(uuidSource.uuid, consignmentStatusInput.consignmentId, userId, a, Timestamp.from(timeSource.now)))
  }
}

object ConsignmentStatusService {
  private val validConsignmentTypes: List[String] =
    List(
      "Series",
      "TransferAgreement",
      "Upload",
      "ClientChecks",
      "DraftMetadata",
      "ClosureMetadata",
      "DescriptiveMetadata",
      "ConfirmTransfer",
      "Export",
      "MetadataReview"
    )
  val validStatusTypes: Set[String] = validConsignmentTypes.toSet ++ Set("ServerFFID", "ServerChecksum", "ServerAntivirus")
  val validStatusValues: Set[String] = Set(InProgressValue.value, CompletedValue.value, CompletedWithIssuesValue.value, FailedValue.value)
}
