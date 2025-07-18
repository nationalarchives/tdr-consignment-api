package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentMetadataRepository, ConsignmentStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{
  AddTransferAgreementComplianceInput,
  AddTransferAgreementPrivateBetaInput,
  TransferAgreementCompliance,
  TransferAgreementPrivateBeta
}
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class TransferAgreementService(
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    consignmentStatusRepository: ConsignmentStatusRepository,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  def addTransferAgreementPrivateBeta(input: AddTransferAgreementPrivateBetaInput, userId: UUID): Future[TransferAgreementPrivateBeta] = {
    for {
      transferAgreementPrivateBeta <- consignmentMetadataRepository
        .addConsignmentMetadata(convertTAPrivateBetaInputToPropertyRows(input, userId))
        .map(rows => convertDbRowsToTransferAgreementPrivateBeta(input.consignmentId, rows))
      _ <- addTransferAgreementStatus(input.consignmentId)
    } yield transferAgreementPrivateBeta
  }

  def addTransferAgreementCompliance(input: AddTransferAgreementComplianceInput, userId: UUID): Future[TransferAgreementCompliance] = {
    for {
      transferAgreementCompliance <- consignmentMetadataRepository
        .addConsignmentMetadata(convertTAComplianceInputToPropertyRows(input, userId))
        .map(rows => convertDbRowsToTransferAgreementCompliance(input.consignmentId, rows))
      _ <- updateExistingTransferAgreementStatus(input.consignmentId, "Completed")
    } yield transferAgreementCompliance
  }

  def addTransferAgreementStatus(consignmentId: UUID): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "TransferAgreement", "InProgress", Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
  }

  def updateExistingTransferAgreementStatus(consignmentId: UUID, statusValue: String): Future[Int] = {
    consignmentStatusRepository.updateConsignmentStatus(consignmentId, "TransferAgreement", statusValue, Timestamp.from(timeSource.now))
  }

  private def convertTAPrivateBetaInputToPropertyRows(input: AddTransferAgreementPrivateBetaInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    Seq(
      ConsignmentmetadataRow(uuidSource.uuid, consignmentId, PublicRecordsConfirmed, input.allPublicRecords.toString, time, userId)
    ) ++
      input.allEnglish.map(allEnglish => ConsignmentmetadataRow(uuidSource.uuid, consignmentId, AllEnglishConfirmed, allEnglish.toString, time, userId) :: Nil).getOrElse(Nil)
  }

  private def convertDbRowsToTransferAgreementPrivateBeta(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): TransferAgreementPrivateBeta = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    TransferAgreementPrivateBeta(
      consignmentId,
      propertyNameToValue(PublicRecordsConfirmed),
      propertyNameToValue.get(AllEnglishConfirmed)
    )
  }

  private def convertTAComplianceInputToPropertyRows(input: AddTransferAgreementComplianceInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    Seq(
      ConsignmentmetadataRow(uuidSource.uuid, consignmentId, AppraisalSelectionSignOffConfirmed, input.appraisalSelectionSignedOff.toString, time, userId),
      ConsignmentmetadataRow(uuidSource.uuid, consignmentId, SensitivityReviewSignOffConfirmed, input.sensitivityReviewSignedOff.toString, time, userId)
    ) ++
      input.initialOpenRecords
        .map(initialOpenRecords => ConsignmentmetadataRow(uuidSource.uuid, consignmentId, InitialOpenRecordsConfirmed, initialOpenRecords.toString, time, userId) :: Nil)
        .getOrElse(Nil)
  }

  private def convertDbRowsToTransferAgreementCompliance(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): TransferAgreementCompliance = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    TransferAgreementCompliance(
      consignmentId,
      propertyNameToValue(AppraisalSelectionSignOffConfirmed),
      propertyNameToValue.get(InitialOpenRecordsConfirmed),
      propertyNameToValue(SensitivityReviewSignOffConfirmed)
    )
  }
}

object TransferAgreementService {
  val AllEnglishConfirmed = "AllEnglishConfirmed"
  val PublicRecordsConfirmed = "PublicRecordsConfirmed"
  val AppraisalSelectionSignOffConfirmed = "AppraisalSelectionSignOffConfirmed"
  val InitialOpenRecordsConfirmed = "InitialOpenRecordsConfirmed"
  val SensitivityReviewSignOffConfirmed = "SensitivityReviewSignOffConfirmed"

  val transferAgreementProperties = List(
    AllEnglishConfirmed,
    PublicRecordsConfirmed,
    AppraisalSelectionSignOffConfirmed,
    InitialOpenRecordsConfirmed,
    SensitivityReviewSignOffConfirmed
  )
}
