package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.MetadatareviewlogRow
import uk.gov.nationalarchives.tdr.api.db.repository.MetadataReviewLogRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.MetadataReviewLog
import uk.gov.nationalarchives.tdr.common.utils.statuses.MetadataReviewLogAction.{Approval, Rejection}

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MetadataReviewServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val metadataReviewLogRepositoryMock: MetadataReviewLogRepository = mock[MetadataReviewLogRepository]

  "getMetadataReviewDetails" should "return all metadata review log entries mapped for the given consignment id" in {
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val logId1 = UUID.randomUUID()
    val logId2 = UUID.randomUUID()
    val eventTime = Timestamp.valueOf("2024-06-01 12:00:00")

    val rows = Seq(
      MetadatareviewlogRow(logId1, consignmentId, userId, Approval.value, eventTime, Some("Looks good")),
      MetadatareviewlogRow(logId2, consignmentId, userId, Rejection.value, eventTime, Some("Needs changes"))
    )

    when(metadataReviewLogRepositoryMock.getEntriesByConsignmentId(consignmentId)).thenReturn(Future.successful(rows))

    val service = new MetadataReviewService(metadataReviewLogRepositoryMock)
    val result = service.getMetadataReviewDetails(consignmentId).futureValue

    result.size shouldBe 2
    result.head.metadataReviewLogId shouldBe logId1
    result.head.consignmentId shouldBe consignmentId
    result.head.userId shouldBe userId
    result.head.action shouldBe Approval.value
    result.head.metadataReviewNotes shouldBe Some("Looks good")
    result(1).metadataReviewLogId shouldBe logId2
    result(1).action shouldBe Rejection.value
    result(1).metadataReviewNotes shouldBe Some("Needs changes")
  }

  "getMetadataReviewDetails" should "return an empty list if no log entries exist for the consignment" in {
    val consignmentId = UUID.randomUUID()

    when(metadataReviewLogRepositoryMock.getEntriesByConsignmentId(consignmentId)).thenReturn(Future.successful(Seq.empty))

    val service = new MetadataReviewService(metadataReviewLogRepositoryMock)
    val result = service.getMetadataReviewDetails(consignmentId).futureValue

    result shouldBe empty
  }

  "getMetadataReviewDetails" should "correctly convert eventTime timestamp to ZonedDateTime" in {
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val logId = UUID.randomUUID()
    val eventTime = Timestamp.valueOf("2024-06-01 12:00:00")

    val rows = Seq(MetadatareviewlogRow(logId, consignmentId, userId, Approval.value, eventTime))

    when(metadataReviewLogRepositoryMock.getEntriesByConsignmentId(consignmentId)).thenReturn(Future.successful(rows))

    val service = new MetadataReviewService(metadataReviewLogRepositoryMock)
    val result: Seq[MetadataReviewLog] = service.getMetadataReviewDetails(consignmentId).futureValue

    result.size shouldBe 1
    result.head.eventTime shouldBe a[ZonedDateTime]
    result.head.eventTime.toInstant shouldBe eventTime.toInstant
    result.head.metadataReviewNotes shouldBe None
  }

  "getMetadataReviewDetails" should "map metadataReviewNotes from row to case class" in {
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val logId = UUID.randomUUID()
    val eventTime = Timestamp.valueOf("2024-06-01 12:00:00")
    val notes = "Some review notes"

    val rows = Seq(MetadatareviewlogRow(logId, consignmentId, userId, Approval.value, eventTime, Some(notes)))

    when(metadataReviewLogRepositoryMock.getEntriesByConsignmentId(consignmentId)).thenReturn(Future.successful(rows))

    val service = new MetadataReviewService(metadataReviewLogRepositoryMock)
    val result: Seq[MetadataReviewLog] = service.getMetadataReviewDetails(consignmentId).futureValue

    result.size shouldBe 1
    result.head.metadataReviewNotes shouldBe Some(notes)
  }
}
