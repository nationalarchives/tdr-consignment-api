package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentmetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.FinalTransferConfirmationFields._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
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
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, Timestamp.from(FixedTimeSource.now), userId)
    val mockResponse = Future.successful(Seq(
      row("FinalOpenRecordsConfirmed", "true"),
      row("LegalOwnershipTransferConfirmed", "true")
    ))

    when(consignmentMetadataRepoMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)

    val service = new FinalTransferConfirmationService(consignmentMetadataRepoMock, fixedUuidSource, FixedTimeSource)
    val result: FinalTransferConfirmation = service.addFinalTransferConfirmation(AddFinalTransferConfirmationInput(
      consignmentId,
      finalOpenRecordsConfirmed = true,
      legalOwnershipTransferConfirmed = true), userId).futureValue

    result.consignmentId shouldBe consignmentId
    result.finalOpenRecordsConfirmed shouldBe true
    result.legalOwnershipTransferConfirmed shouldBe true
  }
  "addConsignmentMetadata" should "create consignment metadata given correct arguments for a judgment user" in {
    val fixedUuidSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUuidSource.uuid
    val consignmentMetadataRepoMock = mock[ConsignmentMetadataRepository]
    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(metadataId, consignmentId, name, value, Timestamp.from(FixedTimeSource.now), userId)
    val mockResponse = Future.successful(Seq(
      row("LegalCustodyTransferConfirmed", "true")
    ))

    when(consignmentMetadataRepoMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(mockResponse)

    val service = new FinalTransferConfirmationService(consignmentMetadataRepoMock, fixedUuidSource, FixedTimeSource)
    val result: FinalJudgmentTransferConfirmation = service.addFinalJudgmentTransferConfirmation(AddFinalJudgmentTransferConfirmationInput(
      consignmentId,
      legalCustodyTransferConfirmed = true), userId).futureValue

    result.consignmentId shouldBe consignmentId
    result.legalCustodyTransferConfirmed shouldBe true
  }
}
