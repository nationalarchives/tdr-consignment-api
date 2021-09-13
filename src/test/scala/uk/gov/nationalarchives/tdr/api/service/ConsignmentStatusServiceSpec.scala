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
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusServiceSpec extends AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val consignmentService = new ConsignmentStatusService(consignmentStatusRepositoryMock, FixedTimeSource)

  "getConsignmentStatus" should
    "return a CurrentStatus object, where all properties have a value, if all consignment status exists for given consignment" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val consignmentStatusRow1 = generateConsignmentStatusRow(consignmentId, "TransferAgreement","Completed")
    val consignmentStatusRow2 = generateConsignmentStatusRow(consignmentId, "Upload","Completed")

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow1, consignmentStatusRow2))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.transferAgreement should be(Some("Completed"))
    response.upload should be(Some("Completed"))
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the transferAgreement property has a value, while others have a value of 'None',
      | if only the transferAgreement consignment status exists for given consignment""".stripMargin in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val consignmentStatusRow = generateConsignmentStatusRow(consignmentId, "TransferAgreement","Completed")

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.transferAgreement should be(Some("Completed"))
    response.upload should be(None)
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the upload property has a value, while others have a value of 'None',
      | if only one consignment status exists for given consignment""".stripMargin in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val dateTime = Timestamp.from(FixedTimeSource.now)

    val mockConsignmentStatusResponse = Future.successful(Seq(
      generateConsignmentStatusRow(consignmentId, "Upload", "InProgress")
    ))

    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockConsignmentStatusResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.transferAgreement should be(None)
    response.upload should be(Some("InProgress"))
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where all properties have a value of 'None',
      | if consignment statuses don't exist for given consignment""".stripMargin in {
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
  private def generateConsignmentStatusRow(consignmentId: UUID, statusType: String, statusValue: String): ConsignmentstatusRow = {
    ConsignmentstatusRow(
      UUID.randomUUID(),
      consignmentId,
      statusType,
      statusValue,
      Timestamp.from(FixedTimeSource.now)
    )
  }
}
