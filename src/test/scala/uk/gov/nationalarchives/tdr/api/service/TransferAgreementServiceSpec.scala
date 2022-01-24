package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.util.UUID
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentMetadataRepository, ConsignmentStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{
  AddTransferAgreementComplianceInput,
  AddTransferAgreementNotComplianceInput,
  TransferAgreementCompliance,
  TransferAgreementNotCompliance
}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.time.Instant.now
import scala.concurrent.{ExecutionContext, Future}

class TransferAgreementServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fixedUuidSource = new FixedUUIDSource()
  val fixedTimeSource: FixedTimeSource.type = FixedTimeSource

  "addTransferAgreementNotCompliance" should "add the correct metadata given correct arguments and set TA status to InProgress" in {

    val consignmentMetadataRepositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    val metadataId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val consignmentStatusId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val dateTime = Timestamp.from(FixedTimeSource.now)
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, dateTime, userId)
    val mockResponse = Future.successful(Seq(
      row("AllEnglishConfirmed", "true"),
      row("CrownCopyrightConfirmed", "true"),
      row("PublicRecordsConfirmed", "true")
    ))
    val statusType = "TransferAgreement"
    val statusValue = "InProgress"

    val mockTaConsignmentStatus = ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, dateTime, None)
    val mockTaConsignmentStatusResponse = Future.successful(Seq(mockTaConsignmentStatus))

    when(consignmentMetadataRepositoryMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(Future.successful(mockTaConsignmentStatus))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockTaConsignmentStatusResponse)

    val service = new TransferAgreementService(consignmentMetadataRepositoryMock, consignmentStatusRepositoryMock, fixedUuidSource, fixedTimeSource)
    val transferAgreementResult: TransferAgreementNotCompliance = service.addTransferAgreementNotCompliance(
      AddTransferAgreementNotComplianceInput(
        consignmentId,
        allCrownCopyright = true,
        allEnglish = true,
        allPublicRecords = true),
      userId
    ).futureValue

    val consignmentStatusResult: Seq[ConsignmentstatusRow] =
      consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId).futureValue

    transferAgreementResult.consignmentId shouldBe consignmentId
    transferAgreementResult.allCrownCopyright shouldBe true
    transferAgreementResult.allEnglish shouldBe true
    transferAgreementResult.allPublicRecords shouldBe true

    consignmentStatusResult.head.consignmentid shouldBe consignmentId
    consignmentStatusResult.head.consignmentstatusid shouldBe consignmentStatusId
    consignmentStatusResult.head.statustype shouldBe statusType
    consignmentStatusResult.head.value shouldBe statusValue
  }

  "addTransferAgreementCompliance" should "add the correct metadata given correct arguments and set TA status to Completed" in {

    val consignmentMetadataRepositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    val metadataId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val consignmentStatusId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val dateTime = Timestamp.from(FixedTimeSource.now)
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, dateTime, userId)
    val mockResponse = Future.successful(Seq(
      row("AppraisalSelectionSignOffConfirmed", "true"),
      row("InitialOpenRecordsConfirmed", "true"),
      row("SensitivityReviewSignOffConfirmed", "true")
    ))
    val statusType = "TransferAgreement"
    val statusValue = "Completed"

    val mockTaConsignmentStatus = ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, dateTime, None)
    val mockTaConsignmentStatusResponse = Future.successful(Seq(mockTaConsignmentStatus))

    when(consignmentMetadataRepositoryMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(Future.successful(mockTaConsignmentStatus))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockTaConsignmentStatusResponse)

    val service = new TransferAgreementService(consignmentMetadataRepositoryMock, consignmentStatusRepositoryMock, fixedUuidSource, fixedTimeSource)
    val transferAgreementResult: TransferAgreementCompliance = service.addTransferAgreementCompliance(
      AddTransferAgreementComplianceInput(
        consignmentId,
        initialOpenRecords = true,
        appraisalSelectionSignedOff = true,
        sensitivityReviewSignedOff = true),
      userId
    ).futureValue

    val consignmentStatusResult: Seq[ConsignmentstatusRow] =
      consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId).futureValue

    transferAgreementResult.consignmentId shouldBe consignmentId
    transferAgreementResult.initialOpenRecords shouldBe true
    transferAgreementResult.appraisalSelectionSignedOff shouldBe true
    transferAgreementResult.sensitivityReviewSignedOff shouldBe true

    consignmentStatusResult.head.consignmentid shouldBe consignmentId
    consignmentStatusResult.head.consignmentstatusid shouldBe consignmentStatusId
    consignmentStatusResult.head.statustype shouldBe statusType
    consignmentStatusResult.head.value shouldBe statusValue
  }

  "addTransferAgreementStatus" should "add the correct Transfer Agreement Status" in {
    val consignmentMetadataRepositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    val consignmentId = UUID.randomUUID()
    val consignmentStatusId = UUID.fromString("d2f2c8d8-2e1d-4996-8ad2-b26ed547d1aa")
    val statusType = "TransferAgreement"
    val statusValue = "Completed"
    val createdTimestamp = Timestamp.from(now)

    val mockResponse = Future.successful(ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, createdTimestamp))
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(mockResponse)

    val service = new TransferAgreementService(consignmentMetadataRepositoryMock, consignmentStatusRepositoryMock, fixedUuidSource, fixedTimeSource)
    val result: ConsignmentstatusRow = service.addTransferAgreementStatus(consignmentId, statusValue).futureValue

    result.consignmentstatusid shouldBe consignmentStatusId
    result.consignmentid shouldBe consignmentId
    result.statustype shouldBe statusType
    result.value shouldBe statusValue
    result.createddatetime shouldBe createdTimestamp
  }
}
