package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentmetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentMetadata, ConsignmentMetadataFilter}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentMetadataFields.{AddOrUpdateConsignmentMetadata, AddOrUpdateConsignmentMetadataInput}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val fixedUuidSource = new FixedUUIDSource()
  val fixedTimeSource: FixedTimeSource.type = FixedTimeSource

  "addTransferAgreementPrivateBeta" should "add the correct metadata given correct arguments and set TA status to InProgress" in {

    val consignmentMetadataRepositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val dateTime = Timestamp.from(FixedTimeSource.now)

    def row(name: String, value: String): ConsignmentmetadataRow =
      ConsignmentmetadataRow(UUID.randomUUID(), consignmentId, name, value, dateTime, userId)

    when(consignmentMetadataRepositoryMock.deleteConsignmentMetadata(consignmentId, Set("JudgmentType", "PublicRecordsConfirmed"))).thenReturn(Future.successful(2))
    when(consignmentMetadataRepositoryMock.addConsignmentMetadata(any[Seq[ConsignmentmetadataRow]])).thenReturn(
      Future.successful(
        Seq(
          row("JudgmentType", "Judgment"),
          row("PublicRecordsConfirmed", "true")
        )
      )
    )

    val addOrUpdateConsignmentMetadataInput = AddOrUpdateConsignmentMetadataInput(
      consignmentId,
      Seq(
        AddOrUpdateConsignmentMetadata("JudgmentType", "Judgment"),
        AddOrUpdateConsignmentMetadata("PublicRecordsConfirmed", "true")
      )
    )
    val service = new ConsignmentMetadataService(consignmentMetadataRepositoryMock, fixedUuidSource, fixedTimeSource)
    val result = service
      .addOrUpdateConsignmentMetadata(addOrUpdateConsignmentMetadataInput, userId)
      .futureValue

    result.size shouldBe 2
  }

  "getConsignmentMetadata" should "return metadata for a consignment with filter" in {
    val repositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentId = UUID.randomUUID()
    val filter = Some(ConsignmentMetadataFilter(List("Property1")))
    val rows = Seq(
      ConsignmentmetadataRow(UUID.randomUUID(), consignmentId, "Property1", "Value1", Timestamp.valueOf("2024-06-01 12:00:00"), UUID.randomUUID())
    )
    when(repositoryMock.getConsignmentMetadata(consignmentId, filter)).thenReturn(Future.successful(rows))

    val service = new ConsignmentMetadataService(repositoryMock, null, null)
    val result = service.getConsignmentMetadata(consignmentId, filter).futureValue

    result should contain theSameElementsAs Seq(
      ConsignmentMetadata("Property1", "Value1")
    )
  }

  "getConsignmentMetadata" should "return an empty list if no metadata exists" in {
    val repositoryMock = mock[ConsignmentMetadataRepository]
    val consignmentId = UUID.randomUUID()
    when(repositoryMock.getConsignmentMetadata(consignmentId, None)).thenReturn(Future.successful(Seq.empty))

    val service = new ConsignmentMetadataService(repositoryMock, null, null)
    val result = service.getConsignmentMetadata(consignmentId, None).futureValue

    result shouldBe empty
  }
}
