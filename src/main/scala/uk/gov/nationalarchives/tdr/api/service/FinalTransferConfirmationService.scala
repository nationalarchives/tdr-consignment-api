package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.ConsignmentmetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FinalTransferConfirmationFields._
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService.{FinalOpenRecordsConfirmed, LegalCustodyTransferConfirmed,
  LegalOwnershipTransferConfirmed}
import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}


class FinalTransferConfirmationService(consignmentMetadataRepository: ConsignmentMetadataRepository,
                                       uuidSource: UUIDSource,
                                       timeSource: TimeSource
                                      )(implicit val executionContext: ExecutionContext) {

  def addFinalTransferConfirmation(consignmentMetadataInputs: AddFinalTransferConfirmationInput, userId: UUID): Future[FinalTransferConfirmation] = {
    consignmentMetadataRepository.addConsignmentMetadata(convertInputToPropertyRows(consignmentMetadataInputs, userId)).map {
      rows => convertDbRowsToFinalTransferConfirmation(consignmentMetadataInputs.consignmentId, rows)
    }
  }

  def addFinalJudgmentTransferConfirmation(consignmentMetadataInputs: AddFinalJudgmentTransferConfirmationInput,
                                           userId: UUID): Future[FinalJudgmentTransferConfirmation] = {
    consignmentMetadataRepository.addConsignmentMetadata(convertInputToPropertyRows(consignmentMetadataInputs, userId)).map {
      rows => convertDbRowsToFinalJudgmentTransferConfirmation(consignmentMetadataInputs.consignmentId, rows)
    }
  }

  private def convertInputToPropertyRows[A](inputs: A, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    inputs match {
      case standard: AddFinalTransferConfirmationInput =>
        Seq(
          ConsignmentmetadataRow(
            uuidSource.uuid, standard.consignmentId, FinalOpenRecordsConfirmed, standard.finalOpenRecordsConfirmed.toString, time, userId),
          ConsignmentmetadataRow(
            uuidSource.uuid, standard.consignmentId, LegalOwnershipTransferConfirmed, standard.legalOwnershipTransferConfirmed.toString, time, userId)
        )
      case judgment: AddFinalJudgmentTransferConfirmationInput =>
        Seq(
          ConsignmentmetadataRow(
            uuidSource.uuid, judgment.consignmentId, LegalCustodyTransferConfirmed, judgment.legalCustodyTransferConfirmed.toString, time, userId)
        )
    }
  }

  private def convertDbRowsToFinalTransferConfirmation(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): FinalTransferConfirmation = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    FinalTransferConfirmation(consignmentId,
      propertyNameToValue(FinalOpenRecordsConfirmed),
      propertyNameToValue(LegalOwnershipTransferConfirmed)
    )
  }

  private def convertDbRowsToFinalJudgmentTransferConfirmation(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): FinalJudgmentTransferConfirmation = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    FinalJudgmentTransferConfirmation(consignmentId,
      propertyNameToValue(LegalCustodyTransferConfirmed)
    )
  }

}

object FinalTransferConfirmationService {
  val FinalOpenRecordsConfirmed = "FinalOpenRecordsConfirmed"
  val LegalOwnershipTransferConfirmed = "LegalOwnershipTransferConfirmed"
  val LegalCustodyTransferConfirmed = "LegalCustodyTransferConfirmed"

  val finalTransferConfirmationProperties = List(
    FinalOpenRecordsConfirmed,
    LegalOwnershipTransferConfirmed
  )

  val finalJudgmentTransferConfirmationProperties = List(
    LegalCustodyTransferConfirmed
  )
}
