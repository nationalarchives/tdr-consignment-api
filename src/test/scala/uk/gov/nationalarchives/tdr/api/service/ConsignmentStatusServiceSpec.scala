package uk.gov.nationalarchives.tdr.api.service

import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.mockito.ArgumentMatchers.any
import org.mockito.scalatest.ResetMocksAfterEachTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.UpdateConsignmentStatusInput
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
    val consignmentStatusRow1 = generateConsignmentStatusRow(consignmentId, "Series","Completed")
    val consignmentStatusRow2 = generateConsignmentStatusRow(consignmentId, "TransferAgreement","Completed")
    val consignmentStatusRow3 = generateConsignmentStatusRow(consignmentId, "Upload","Completed")
    val consignmentStatusRow4 = generateConsignmentStatusRow(consignmentId, "ConfirmTransfer","Completed")
    val consignmentStatusRow5 = generateConsignmentStatusRow(consignmentId, "Export","Completed")

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] =
      Future.successful(Seq(consignmentStatusRow1, consignmentStatusRow2, consignmentStatusRow3, consignmentStatusRow4, consignmentStatusRow5))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.series should be(Some("Completed"))
    response.transferAgreement should be(Some("Completed"))
    response.upload should be(Some("Completed"))
    response.confirmTransfer should be(Some("Completed"))
    response.export should be(Some("Completed"))
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the series property has a value, while others have a value of 'None',
      | if only the series status exists for given consignment""".stripMargin in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid

    val mockConsignmentStatusResponse = Future.successful(Seq(
      generateConsignmentStatusRow(consignmentId, "Series", "Complete")
    ))

    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockConsignmentStatusResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.series should be(Some("Complete"))
    response.transferAgreement should be(None)
    response.upload should be(None)
    response.confirmTransfer should be(None)
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

    response.series should be(None)
    response.transferAgreement should be(Some("Completed"))
    response.upload should be(None)
    response.confirmTransfer should be(None)
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

    response.series should be(None)
    response.transferAgreement should be(None)
    response.upload should be(Some("InProgress"))
    response.confirmTransfer should be(None)
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the confirmTransfer property has a value, while others have a value of 'None',
      | if only one consignment status exists for given consignment""".stripMargin in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val consignmentStatusRow = generateConsignmentStatusRow(consignmentId, "ConfirmTransfer","Completed")

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow))
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response.series should be(None)
    response.transferAgreement should be(None)
    response.upload should be(None)
    response.confirmTransfer should be(Some("Completed"))
  }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where all properties have a value of 'None',
      | if consignment statuses don't exist for given consignment""".stripMargin in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val mockRepoResponse = Future.successful(Seq())
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response = consignmentService.getConsignmentStatus(consignmentId).futureValue

    response should be(CurrentStatus(None, None, None, None, None))
  }

  "updateConsignmentStatus" should "pass the correct consignment status and value to the repository method" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val expectedStatusValue = "Completed"
    val modifiedTime = Timestamp.from(FixedTimeSource.now)
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val statusTypeCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val statusValueCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val mockRepoResponse: Future[Int] = Future.successful(1)
    when(consignmentStatusRepositoryMock.updateConsignmentStatus(
      consignmentIdCaptor.capture(),
      statusTypeCaptor.capture(),
      statusValueCaptor.capture(),
      any[Timestamp]
      )
    ).thenReturn(mockRepoResponse)

    val updateConsignmentStatusInput =
      UpdateConsignmentStatusInput(expectedConsignmentId, expectedStatusType, expectedStatusValue)

    val response: Int = consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue

    response should be(1)
    consignmentIdCaptor.getValue should equal(expectedConsignmentId)
    statusTypeCaptor.getValue should equal(expectedStatusType)
    statusValueCaptor.getValue should equal(expectedStatusValue)
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusType has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "Completed"

    val updateConsignmentStatusInput =
      UpdateConsignmentStatusInput(expectedConsignmentId, expectedStatusType, expectedStatusValue)

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid updateConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusValue has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      UpdateConsignmentStatusInput(expectedConsignmentId, expectedStatusType, expectedStatusValue)

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid updateConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusType and statusValue have been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      UpdateConsignmentStatusInput(expectedConsignmentId, expectedStatusType, expectedStatusValue)

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid updateConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
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
