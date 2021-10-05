package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentMetadataRepository, ConsignmentStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{AddTransferAgreementInput, TransferAgreement}
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService._

import java.sql.{SQLException, Timestamp}
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

  def addTransferAgreementStatus(consignmentId: UUID, statusValue: String): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "TransferAgreement", statusValue, Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
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

  def getTransferAgreement(consignmentId: UUID): Future[TransferAgreement] = {
    consignmentMetadataRepository.getConsignmentMetadata(
      consignmentId, transferAgreementProperties: _*).map(rows => convertDbRowsToTransferAgreement(consignmentId, rows))
      .recover {
        case nse: NoSuchElementException => throw InputDataException(s"Could not find metadata for consignment $consignmentId", Some(nse))
        case e: SQLException => throw InputDataException(e.getLocalizedMessage, Some(e))
      }
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
