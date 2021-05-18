package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.mockito.scalatest.ResetMocksAfterEachTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{BodyRow, ConsignmentRow, SeriesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentRepository, FFIDMetadataRepository, FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{AddConsignmentInput, FileChecks, UpdateExportLocationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.{ConsignmentFields, SeriesFields}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentReference
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentServiceSpec extends AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val fixedUuidSource: UUIDSource = mock[UUIDSource]
  val bodyId: UUID = UUID.fromString("8eae8ed8-201c-11eb-adc1-0242ac120002")
  val userId: UUID = UUID.fromString("8d415358-f68b-403b-a90a-daab3fd60109")
  val seriesId: UUID = UUID.fromString("b6b19341-8c33-4272-8636-aafa1e3d98de")
  val consignmentId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val seriesName: Option[String] = Option("Mock series")
  val seriesCode: Option[String] = Option("Mock series")
  val seriesDescription: Option[String] = Option("Series description")
  val bodyName: Option[String] = Option("Mock department")
  val bodyCode: Option[String] = Option("Mock department")
  val bodyDescription: Option[String] = Option("Body description")
  //scalastyle:off magic.number
  val consignmentSequence: Long = 400L
  //scalastyle:on magic.number
  val consignmentReference = "TDR-2020-VB"
  val mockConsignment: ConsignmentRow = ConsignmentRow(
    consignmentId,
    seriesId,
    userId,
    Timestamp.from(FixedTimeSource.now),
    consignmentsequence = consignmentSequence,
    consignmentreference = consignmentReference
  )

  val consignmentRepoMock: ConsignmentRepository = mock[ConsignmentRepository]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val mockResponse: Future[ConsignmentRow] = Future.successful(mockConsignment)
  val consignmentService = new ConsignmentService(consignmentRepoMock,
    fileMetadataRepositoryMock,
    fileRepositoryMock,
    ffidMetadataRepositoryMock,
    FixedTimeSource,
    fixedUuidSource)

  "addConsignment" should "create a consignment given correct arguments" in {
    val mockConsignmentSeq = 5L
    when(consignmentRepoMock.getNextConsignmentSequence).thenReturn(Future.successful(mockConsignmentSeq))
    when(consignmentRepoMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)

    val result = consignmentService.addConsignment(AddConsignmentInput(seriesId), userId).futureValue

    result.consignmentid shouldBe consignmentId
    result.seriesid shouldBe seriesId
    result.userid shouldBe userId
  }

  "addConsignment" should "link a consignment to the user's ID" in {
    when(consignmentRepoMock.getNextConsignmentSequence).thenReturn(Future.successful(consignmentSequence))
    when(consignmentRepoMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)
    when(fixedUuidSource.uuid).thenReturn(consignmentId)
    consignmentService.addConsignment(AddConsignmentInput(seriesId), userId).futureValue

    verify(consignmentRepoMock).addConsignment(mockConsignment)
  }


  "getConsignment" should "return the specific Consignment for the requested consignment id" in {
    val consignmentRow = mockConsignment
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq(consignmentRow))
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val response: Option[ConsignmentFields.Consignment] = consignmentService.getConsignment(consignmentId).futureValue

    verify(consignmentRepoMock, times(1)).getConsignment(any[UUID])
    val consignment: ConsignmentFields.Consignment = response.get
    consignment.consignmentid should equal(consignmentId)
    consignment.seriesid should equal(seriesId)
    consignment.userid should equal(userId)
  }

  "getConsignment" should "return none when consignment id does not exist" in {
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val response: Option[ConsignmentFields.Consignment] = consignmentService.getConsignment(UUID.randomUUID()).futureValue
    verify(consignmentRepoMock, times(1)).getConsignment(any[UUID])

    response should be(None)
  }

  "consignmentHasFiles" should "return true when files already associated with provided consignment id" in {
    val mockResponse: Future[Boolean] = Future.successful(true)
    when(consignmentRepoMock.consignmentHasFiles(consignmentId)).thenReturn(mockResponse)

    val response: Boolean = consignmentService.consignmentHasFiles(consignmentId).futureValue
    response should be(true)
  }

  "consignmentHasFiles" should "return false when no files associated with provided consignment id" in {
    val mockResponse: Future[Boolean] = Future.successful(false)
    when(consignmentRepoMock.consignmentHasFiles(consignmentId)).thenReturn(mockResponse)

    val response: Boolean = consignmentService.consignmentHasFiles(consignmentId).futureValue
    response should be(false)
  }

  "getConsignmentFileProgress" should "return total processed files" in {
    val filesProcessed = 78
    when(fileRepositoryMock.countProcessedAvMetadataInConsignment(consignmentId)).thenReturn(Future.successful(filesProcessed))
    when(fileMetadataRepositoryMock.countProcessedChecksumInConsignment(consignmentId)).thenReturn(Future.successful(filesProcessed))
    when(ffidMetadataRepositoryMock.countProcessedFfidMetadata(consignmentId)).thenReturn(Future.successful(filesProcessed))

    val progress: FileChecks = consignmentService.getConsignmentFileProgress(consignmentId).futureValue
    progress.antivirusProgress.filesProcessed shouldBe filesProcessed
    progress.checksumProgress.filesProcessed shouldBe filesProcessed
    progress.ffidProgress.filesProcessed shouldBe filesProcessed
  }

  "getConsignmentParentFolder" should "return the parent folder name for a given consignment" in {
    val parentFolder: Option[String] = Option("CONSIGNMENT SERVICE PARENT FOLDER TEST")
    when(consignmentRepoMock.getParentFolder(consignmentId)).thenReturn(Future.successful(parentFolder))

    val parentFolderResult: Option[String] = consignmentService.getConsignmentParentFolder(consignmentId).futureValue
    parentFolderResult shouldBe parentFolder
  }

  "updateExportLocation" should "update the export location for a given consignment" in {
    val consignmentRepoMock = mock[ConsignmentRepository]
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val service: ConsignmentService = new ConsignmentService(consignmentRepoMock,
      fileMetadataRepositoryMock,
      fileRepositoryMock,
      ffidMetadataRepositoryMock,
      FixedTimeSource,
      fixedUuidSource)

    val fixedZonedDatetime = ZonedDateTime.ofInstant(FixedTimeSource.now, ZoneOffset.UTC)
    val consignmentId = UUID.fromString("d8383f9f-c277-49dc-b082-f6e266a39618")
    val input = UpdateExportLocationInput(consignmentId, "exportLocation", Some(fixedZonedDatetime))
    when(consignmentRepoMock.updateExportLocation(input)).thenReturn(Future(1))

    val response = service.updateExportLocation(input).futureValue

    response should be(1)
  }

  "updateTransferInitiated" should "update the transfer initiated fields for a given consignment" in {
    val consignmentRepoMock = mock[ConsignmentRepository]
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val service: ConsignmentService = new ConsignmentService(consignmentRepoMock,
      fileMetadataRepositoryMock,
      fileRepositoryMock,
      ffidMetadataRepositoryMock,
      FixedTimeSource,
      fixedUuidSource)

    val consignmentId = UUID.fromString("d8383f9f-c277-49dc-b082-f6e266a39618")
    val userId = UUID.randomUUID()
    when(consignmentRepoMock.updateTransferInitiated(consignmentId, userId, Timestamp.from(FixedTimeSource.now))).thenReturn(Future(1))

    val response = service.updateTransferInitiated(consignmentId, userId).futureValue

    response should be(1)
  }

  "getSeriesOfConsignment" should "return the series for a given consignment" in {
    val mockSeries = Seq(SeriesRow(seriesId, bodyId, seriesName, seriesCode, seriesDescription))
    when(consignmentRepoMock.getSeriesOfConsignment(consignmentId)).thenReturn(Future.successful(mockSeries))

    val series: SeriesFields.Series = consignmentService.getSeriesOfConsignment(consignmentId).futureValue.get
    series.seriesid shouldBe mockSeries.head.seriesid
    series.bodyid shouldBe mockSeries.head.bodyid
    series.name shouldBe mockSeries.head.name
    series.code shouldBe mockSeries.head.code
    series.description shouldBe mockSeries.head.description
  }

  "getTransferringBodyOfConsignment" should "return the transferring body for a given consignment" in {
    val mockBody = Seq(BodyRow(bodyId, bodyName, bodyCode, bodyDescription))
    when(consignmentRepoMock.getTransferringBodyOfConsignment(consignmentId)).thenReturn(Future.successful(mockBody))

    val body: ConsignmentFields.TransferringBody = consignmentService.getTransferringBodyOfConsignment(consignmentId).futureValue.get
    body.name shouldBe mockBody.head.name
  }

}
