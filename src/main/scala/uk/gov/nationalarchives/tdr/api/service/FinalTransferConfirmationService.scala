package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{ConsignmentmetadataRow, ConsignmentstatusRow, MetadatareviewlogRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FinalTransferConfirmationFields._
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService.LegalCustodyTransferConfirmed
import uk.gov.nationalarchives.tdr.api.utils.Confirmation

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FinalTransferConfirmationService(
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    consignmentStatusRepository: ConsignmentStatusRepository,
    metadataReviewLogRepository: MetadataReviewLogRepository,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  def addFinalTransferConfirmation(consignmentMetadataInputs: AddFinalTransferConfirmationInput, userId: UUID): Future[FinalTransferConfirmation] = {
    for {
      consignmentMetadata <- consignmentMetadataRepository
        .addConsignmentMetadata(convertInputToPropertyRows(consignmentMetadataInputs, userId))
        .map(rows => convertDbRowsToFinalTransferConfirmation(consignmentMetadataInputs.consignmentId, rows))
      _ <- addMetadataReviewCompletedLog(consignmentMetadataInputs.consignmentId, userId)
      _ <- addConfirmTransferStatus(consignmentMetadataInputs.consignmentId)
    } yield consignmentMetadata
  }

  def addConfirmTransferStatus(consignmentId: UUID): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "ConfirmTransfer", "Completed", Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
  }

  def addMetadataReviewCompletedLog(consignmentId: UUID, userId: UUID): Future[MetadatareviewlogRow] = {
    val metadataReviewLogRow = MetadatareviewlogRow(uuidSource.uuid, consignmentId, userId, Confirmation.value, Timestamp.from(timeSource.now))
    metadataReviewLogRepository.addLogEntry(metadataReviewLogRow)
  }

  private def convertInputToPropertyRows(inputs: AddFinalTransferConfirmationInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    Seq(ConsignmentmetadataRow(uuidSource.uuid, inputs.consignmentId, LegalCustodyTransferConfirmed, inputs.legalCustodyTransferConfirmed.toString, time, userId))
  }

  private def convertDbRowsToFinalTransferConfirmation(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): FinalTransferConfirmation = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    FinalTransferConfirmation(consignmentId, propertyNameToValue(LegalCustodyTransferConfirmed))
  }

}

object FinalTransferConfirmationService {
  val LegalCustodyTransferConfirmed = "LegalCustodyTransferConfirmed"

  val finalTransferConfirmationProperties = List(
    LegalCustodyTransferConfirmed
  )

  val finalJudgmentTransferConfirmationProperties = List(
    LegalCustodyTransferConfirmed
  )
}
