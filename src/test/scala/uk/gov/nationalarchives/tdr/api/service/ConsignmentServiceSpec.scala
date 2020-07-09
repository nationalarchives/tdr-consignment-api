package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{AddConsignmentInput, Consignment}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  private val fixedConsignmentUUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

  "createConsignment" should "create a consignment given correct arguments" in {
    val fixedUuidSource = new FixedUUIDSource()
    val userUuid = UUID.randomUUID()
    val seriesUuid = UUID.randomUUID()
    val consignmentUuid = fixedUuidSource.uuid
    val consignmentRepositoryMock = mock[ConsignmentRepository]
    val mockResponse = Future.successful(ConsignmentRow(consignmentUuid, seriesUuid, userUuid, Timestamp.from(Instant.now)))
    when(consignmentRepositoryMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)
    val consignmentService = new ConsignmentService(consignmentRepositoryMock, FixedTimeSource, fixedUuidSource)
    val result: Consignment = consignmentService.addConsignment(AddConsignmentInput(seriesUuid), Some(userUuid)).futureValue
    result.consignmentid shouldBe Some(consignmentUuid)
    result.seriesid shouldBe seriesUuid
    result.userid shouldBe userUuid
    result.consignmentid shouldBe defined
    result.consignmentid.get shouldBe consignmentUuid
  }

  "createConsignment" should "link a consignment to the user's ID" in {
    val fixedUuidSource = new FixedUUIDSource()
    val userUuid = UUID.randomUUID()
    val seriesUuid = UUID.randomUUID()
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val consignmentRepositoryMock = mock[ConsignmentRepository]
    val consignmentService = new ConsignmentService(consignmentRepositoryMock, FixedTimeSource, fixedUuidSource)


    val expectedRow = ConsignmentRow(consignmentId, seriesUuid, userUuid, Timestamp.from(FixedTimeSource.now))
    val mockResponse = Future.successful(ConsignmentRow(consignmentId, seriesUuid, userUuid, Timestamp.from(Instant.now)))
    when(consignmentRepositoryMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)

    consignmentService.addConsignment(AddConsignmentInput(seriesUuid), Some(userUuid)).futureValue

    verify(consignmentRepositoryMock).addConsignment(expectedRow)
  }

  "getConsignment" should "return the specfic Consignment for the requested consignment id" in {
    val fixedUuidSource = new FixedUUIDSource()
    val userUuid = UUID.randomUUID()
    val seriesUuid = UUID.randomUUID()
    val consignmentUuid = UUID.randomUUID()
    val consignmentRow = ConsignmentRow(consignmentUuid, seriesUuid, userUuid, Timestamp.from(Instant.now))
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq(consignmentRow))
    val consignmentRepoMock = mock[ConsignmentRepository]
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val consignmentService: ConsignmentService = new ConsignmentService(consignmentRepoMock, FixedTimeSource, fixedUuidSource)
    val response: Option[ConsignmentFields.Consignment] = consignmentService.getConsignment(consignmentUuid).futureValue

    verify(consignmentRepoMock, times(1)).getConsignment(any[UUID])
    val consignment: ConsignmentFields.Consignment = response.get
    consignment.consignmentid should equal(Some(consignmentUuid))
    consignment.seriesid should equal(seriesUuid)
    consignment.userid should equal(userUuid)
  }

  "getConsignment" should "return none when consignment id does not exist" in {
    val fixedUuidSource = new FixedUUIDSource()
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    val consignmentRepoMock = mock[ConsignmentRepository]
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val consignmentService: ConsignmentService = new ConsignmentService(consignmentRepoMock, FixedTimeSource, fixedUuidSource)
    val response: Option[ConsignmentFields.Consignment] = consignmentService.getConsignment(UUID.randomUUID()).futureValue
    verify(consignmentRepoMock, times(1)).getConsignment(any[UUID])

    response should be(None)
  }

  "consignmentHasFiles" should "return true when files already associated with provided consignment id" in {
    val fixedUuidSource = new FixedUUIDSource()
    val mockResponse: Future[Boolean] = Future.successful(true)
    val consignmentRepoMock = mock[ConsignmentRepository]
    when(consignmentRepoMock.consignmentHasFiles(fixedConsignmentUUID)).thenReturn(mockResponse)

    val consignmentService: ConsignmentService = new ConsignmentService(consignmentRepoMock, FixedTimeSource, fixedUuidSource)
    val response: Boolean = consignmentService.consignmentHasFiles(fixedConsignmentUUID).futureValue

    response should be (false)
  }

  "consignmentHasFiles" should "return false when no files associated with provided consignment id" in {
    val fixedUuidSource = new FixedUUIDSource()
    val mockResponse: Future[Boolean] = Future.successful(false)
    val consignmentRepoMock = mock[ConsignmentRepository]
    when(consignmentRepoMock.consignmentHasFiles(fixedConsignmentUUID)).thenReturn(mockResponse)

    val consignmentService: ConsignmentService = new ConsignmentService(consignmentRepoMock, FixedTimeSource, fixedUuidSource)
    val response: Boolean = consignmentService.consignmentHasFiles(fixedConsignmentUUID).futureValue

    response should be(false)
  }
}
