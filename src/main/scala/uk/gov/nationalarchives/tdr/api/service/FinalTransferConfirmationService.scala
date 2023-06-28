package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{ConsignmentmetadataRow, ConsignmentstatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FinalTransferConfirmationFields.{FinalOpenRecordsConfirmed, AddFinalTransferConfirmationInput, FinalTransferConfirmation}
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService.LegalCustodyTransferConfirmed
import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FinalTransferConfirmationService(
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    consignmentStatusRepository: ConsignmentStatusRepository,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  def addFinalTransferConfirmation(
      consignmentMetadataInputs: AddFinalTransferConfirmationInput,
      userId: UUID,
      finalOpenRecordsConfirmed: Option[Boolean] = None
  ): Future[FinalTransferConfirmation] = {
    for {
      consignmentMetadata <- consignmentMetadataRepository
        .addConsignmentMetadata(convertInputToPropertyRows(consignmentMetadataInputs, userId, finalOpenRecordsConfirmed))
        .map(rows => convertDbRowsToFinalTransferConfirmation(consignmentMetadataInputs.consignmentId, rows))
      _ <- addConfirmTransferStatus(consignmentMetadataInputs.consignmentId)
    } yield consignmentMetadata
  }

  private def convertInputToPropertyRows[A](inputs: A, userId: UUID, finalOpenRecordsConfirmed: Option[Boolean] = None): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    inputs match {
      case standard: AddFinalTransferConfirmationInput =>
        val standardRows = Seq(
          ConsignmentmetadataRow(uuidSource.uuid, standard.consignmentId, LegalCustodyTransferConfirmed, standard.legalCustodyTransferConfirmed.toString, time, userId)
        )
        finalOpenRecordsConfirmed
          .map { confirmed =>
            standardRows :+ ConsignmentmetadataRow(uuidSource.uuid, standard.consignmentId, FinalOpenRecordsConfirmed.toString(), confirmed.toString, time, userId)
          }
          .getOrElse(standardRows)
      case judgment:
        AddFinalTransferConfirmationInput =>
        Seq(
          ConsignmentmetadataRow(uuidSource.uuid, judgment.consignmentId, LegalCustodyTransferConfirmed, judgment.legalCustodyTransferConfirmed.toString, time, userId)
        )
    }
  }

  def addConfirmTransferStatus(consignmentId: UUID): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "ConfirmTransfer", "Completed", Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
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
