package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.mockito.scalatest.ResetMocksAfterEachTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusServiceSpec extends AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val consignmentService = new ConsignmentStatusService(consignmentStatusRepositoryMock, FixedTimeSource)

  "getConsignmentStatus" should "turn repository response into current status object" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentStatusId = fixedUUIDSource.uuid
    val consignmentId = fixedUUIDSource.uuid
    val consignmentStatusRow = ConsignmentstatusRow(
      consignmentStatusId,
      consignmentId,
      "Upload",
      "InProgress",
      Timestamp.from(FixedTimeSource.now)
    )

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.upload should be(Some("InProgress"))
  }

  "getConsignmentStatus" should "return a CurrentStatus object of type 'None' if a consignment status doesn't exist for given consignment" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val mockRepoResponse = Future.successful(Seq())
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response should be(CurrentStatus(None, None))
  }

  "setUploadConsignmentStatusValueToComplete" should "update a consignments' status when upload is complete" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val statusType = "Upload"
    val statusValue = "Completed"
    val modifiedTime = Timestamp.from(FixedTimeSource.now)

    val mockRepoResponse: Future[Int] = Future.successful(1)
    when(consignmentStatusRepositoryMock.updateConsignmentStatus(consignmentId, statusType, statusValue, modifiedTime))
      .thenReturn(mockRepoResponse)

    val response: Int = consignmentService.setUploadConsignmentStatusValueToComplete(consignmentId).futureValue

    response should be(1)
  }
}
