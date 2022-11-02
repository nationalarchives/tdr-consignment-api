package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{ConsignmentmetadataRow, ConsignmentstatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentMetadataRepository, ConsignmentStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FinalTransferConfirmationFields._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant.now
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FinalTransferConfirmationServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val consignmentId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val userId: UUID = UUID.fromString("8d415358-f68b-403b-a90a-daab3fd60109")

  "addConsignmentMetadata" should "create consignment metadata given correct arguments" in {
    val fixedUuidSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUuidSource.uuid
    val consignmentMetadataRepoMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, Timestamp.from(FixedTimeSource.now), userId)
    val mockResponse = Future.successful(
      Seq(
        row("FinalOpenRecordsConfirmed", "true"),
        row("LegalCustodyTransferConfirmed", "true")
      )
    )
    val consignmentStatusId = UUID.fromString("d2f2c8d8-2e1d-4996-8ad2-b26ed547d1aa")
    val statusType = "ConfirmTransfer"
    val statusValue = "Complete"
    val createdTimestamp = Timestamp.from(now)

    val mockConsignmentStatusResponse = Future.successful(ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, createdTimestamp))
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(mockConsignmentStatusResponse)

    when(consignmentMetadataRepoMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)

    val service = new FinalTransferConfirmationService(consignmentMetadataRepoMock, consignmentStatusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val result: FinalTransferConfirmation = service
      .addFinalTransferConfirmation(AddFinalTransferConfirmationInput(consignmentId, finalOpenRecordsConfirmed = true, legalCustodyTransferConfirmed = true), userId)
      .futureValue

    result.consignmentId shouldBe consignmentId
    result.finalOpenRecordsConfirmed shouldBe true
    result.legalCustodyTransferConfirmed shouldBe true
  }
  "addConsignmentMetadata" should "create consignment metadata given correct arguments for a judgment user" in {
    val fixedUuidSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUuidSource.uuid
    val consignmentMetadataRepoMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, Timestamp.from(FixedTimeSource.now), userId)
    val mockResponse = Future.successful(
      Seq(
        row("LegalCustodyTransferConfirmed", "true")
      )
    )

    when(consignmentMetadataRepoMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)

    val service = new FinalTransferConfirmationService(consignmentMetadataRepoMock, consignmentStatusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val result: FinalJudgmentTransferConfirmation =
      service.addFinalJudgmentTransferConfirmation(AddFinalJudgmentTransferConfirmationInput(consignmentId, legalCustodyTransferConfirmed = true), userId).futureValue

    result.consignmentId shouldBe consignmentId
    result.legalCustodyTransferConfirmed shouldBe true
  }

  "addConfirmTransferStatus" should "add the correct Confirm Transfer Status" in {
    val consignmentMetadataRepositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentStatusRepositoryMock = mock[ConsignmentStatusRepository]
    val consignmentId = UUID.randomUUID()
    val consignmentStatusId = UUID.fromString("d2f2c8d8-2e1d-4996-8ad2-b26ed547d1aa")
    val statusType = "ConfirmTransfer"
    val statusValue = "Complete"
    val createdTimestamp = Timestamp.from(now)
    val fixedUuidSource = new FixedUUIDSource()
    val fixedTimeSource: FixedTimeSource.type = FixedTimeSource

    val mockResponse = Future.successful(ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, createdTimestamp))
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(mockResponse)

    val service = new FinalTransferConfirmationService(consignmentMetadataRepositoryMock, consignmentStatusRepositoryMock, fixedUuidSource, fixedTimeSource)
    val result: ConsignmentstatusRow = service.addConfirmTransferStatus(consignmentId).futureValue

    result.consignmentstatusid shouldBe consignmentStatusId
    result.consignmentid shouldBe consignmentId
    result.statustype shouldBe statusType
    result.value shouldBe statusValue
    result.createddatetime shouldBe createdTimestamp
  }
}
