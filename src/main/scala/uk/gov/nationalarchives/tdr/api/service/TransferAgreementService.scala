package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentMetadataRepository, ConsignmentStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{
  AddTransferAgreementComplianceInput,
  AddTransferAgreementInput,
  AddTransferAgreementNotComplianceInput,
  TransferAgreement,
  TransferAgreementCompliance,
  TransferAgreementNotCompliance
}
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class TransferAgreementService(consignmentMetadataRepository: ConsignmentMetadataRepository,
                               consignmentStatusRepository: ConsignmentStatusRepository,
                               uuidSource: UUIDSource, timeSource: TimeSource)(implicit val executionContext: ExecutionContext) {

  def addTransferAgreement(input: AddTransferAgreementInput, userId: UUID): Future[TransferAgreement] = {
    for {
      transferAgreement <- consignmentMetadataRepository.addConsignmentMetadata(convertInputToPropertyRows(input, userId)).map(
        rows => convertDbRowsToTransferAgreement(input.consignmentId, rows)
      )
      _ <- addTransferAgreementStatus(input.consignmentId, "Completed")
    } yield transferAgreement
  }

  private def convertInputToPropertyRows(input: AddTransferAgreementInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    Seq(
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, PublicRecordsConfirmed, input.allPublicRecords.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, AllEnglishConfirmed, input.allEnglish.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, AppraisalSelectionSignOffConfirmed, input.appraisalSelectionSignedOff.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, CrownCopyrightConfirmed, input.allCrownCopyright.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, InitialOpenRecordsConfirmed, input.initialOpenRecords.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, SensitivityReviewSignOffConfirmed, input.sensitivityReviewSignedOff.toString, time, userId)
    )
  }

  private def convertDbRowsToTransferAgreement(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): TransferAgreement = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    TransferAgreement(consignmentId,
      propertyNameToValue(PublicRecordsConfirmed),
      propertyNameToValue(CrownCopyrightConfirmed),
      propertyNameToValue(AllEnglishConfirmed),
      propertyNameToValue(AppraisalSelectionSignOffConfirmed),
      propertyNameToValue(InitialOpenRecordsConfirmed),
      propertyNameToValue(SensitivityReviewSignOffConfirmed)
    )
  }

  def addTransferAgreementNotCompliance(input: AddTransferAgreementNotComplianceInput, userId: UUID): Future[TransferAgreementNotCompliance] = {
    for {
      transferAgreementNotCompliance <- consignmentMetadataRepository.addConsignmentMetadata(convertTANotComplianceInputToPropertyRows(input, userId)).map(
        rows => convertDbRowsToTransferAgreementNotCompliance(input.consignmentId, rows)
      )
      _ <- addTransferAgreementStatus(input.consignmentId, "InProgress")
    } yield transferAgreementNotCompliance
  }

  def addTransferAgreementCompliance(input: AddTransferAgreementComplianceInput, userId: UUID): Future[TransferAgreementCompliance] = {
    for {
      transferAgreementCompliance <- consignmentMetadataRepository.addConsignmentMetadata(convertTAComplianceInputToPropertyRows(input, userId)).map(
        rows => convertDbRowsToTransferAgreementCompliance(input.consignmentId, rows)
      )
      _ <- addTransferAgreementStatus(input.consignmentId, "Completed")
    } yield transferAgreementCompliance
  }

  def addTransferAgreementStatus(consignmentId: UUID, statusValue: String): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "TransferAgreement", statusValue, Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
  }

  private def convertTANotComplianceInputToPropertyRows(input: AddTransferAgreementNotComplianceInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    Seq(
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, PublicRecordsConfirmed, input.allPublicRecords.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, AllEnglishConfirmed, input.allEnglish.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, CrownCopyrightConfirmed, input.allCrownCopyright.toString, time, userId)
    )
  }

  private def convertDbRowsToTransferAgreementNotCompliance(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): TransferAgreementNotCompliance = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    TransferAgreementNotCompliance(consignmentId,
      propertyNameToValue(PublicRecordsConfirmed),
      propertyNameToValue(CrownCopyrightConfirmed),
      propertyNameToValue(AllEnglishConfirmed)
    )
  }

  private def convertTAComplianceInputToPropertyRows(input: AddTransferAgreementComplianceInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    Seq(
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, AppraisalSelectionSignOffConfirmed, input.appraisalSelectionSignedOff.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, InitialOpenRecordsConfirmed, input.initialOpenRecords.toString, time, userId),
      ConsignmentmetadataRow(
        uuidSource.uuid, consignmentId, SensitivityReviewSignOffConfirmed, input.sensitivityReviewSignedOff.toString, time, userId)
    )
  }

  private def convertDbRowsToTransferAgreementCompliance(consignmentId: UUID, rows: Seq[ConsignmentmetadataRow]): TransferAgreementCompliance = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value.toBoolean).toMap
    TransferAgreementCompliance(consignmentId,
      propertyNameToValue(AppraisalSelectionSignOffConfirmed),
      propertyNameToValue(InitialOpenRecordsConfirmed),
      propertyNameToValue(SensitivityReviewSignOffConfirmed)
    )
  }
}

object TransferAgreementService {
  val AllEnglishConfirmed = "AllEnglishConfirmed"
  val PublicRecordsConfirmed = "PublicRecordsConfirmed"
  val AppraisalSelectionSignOffConfirmed = "AppraisalSelectionSignOffConfirmed"
  val CrownCopyrightConfirmed = "CrownCopyrightConfirmed"
  val InitialOpenRecordsConfirmed = "InitialOpenRecordsConfirmed"
  val SensitivityReviewSignOffConfirmed = "SensitivityReviewSignOffConfirmed"

  val transferAgreementProperties = List(
    AllEnglishConfirmed,
    PublicRecordsConfirmed,
    AppraisalSelectionSignOffConfirmed,
    InitialOpenRecordsConfirmed,
    CrownCopyrightConfirmed,
    SensitivityReviewSignOffConfirmed
  )
}
