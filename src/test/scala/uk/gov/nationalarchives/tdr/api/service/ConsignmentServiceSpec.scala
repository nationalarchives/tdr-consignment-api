package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.UUID

import com.typesafe.config.ConfigFactory
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

  val fixedTimeSource: Instant = FixedTimeSource.now
  val fixedUuidSource: UUIDSource = mock[UUIDSource]
  val bodyId: UUID = UUID.fromString("8eae8ed8-201c-11eb-adc1-0242ac120002")
  val userId: UUID = UUID.fromString("8d415358-f68b-403b-a90a-daab3fd60109")
  val seriesId: UUID = UUID.fromString("b6b19341-8c33-4272-8636-aafa1e3d98de")
  val consignmentId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val seriesName: String = "Mock series"
  val seriesCode: String = "Mock series"
  val seriesDescription: Option[String] = Option("Series description")
  val bodyName: String = "Mock department"
  val bodyCode: String = "Mock department"
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
    fixedUuidSource,
    ConfigFactory.load())

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

  "getConsignment" should "return the specific consignment for the requested consignment id" in {
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
      fixedUuidSource,
      ConfigFactory.load())

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
      fixedUuidSource,
      ConfigFactory.load())

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
    val mockBody = Seq(BodyRow(bodyId, bodyName, bodyDescription, bodyCode))
    when(consignmentRepoMock.getTransferringBodyOfConsignment(consignmentId)).thenReturn(Future.successful(mockBody))

    val body: ConsignmentFields.TransferringBody = consignmentService.getTransferringBodyOfConsignment(consignmentId).futureValue.get
    body.name shouldBe mockBody.head.name
  }

  "getConsignments" should "return all the consignments after the cursor to the limit" in {
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val consignmentId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val consignmentRowParams = List(
      (consignmentId2, "consignment-ref2", 2L),
      (consignmentId3, "consignment-ref3", 3L)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be (Some("consignment-ref3"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain ("consignment-ref2")
    cursors should contain ("consignment-ref3")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain (consignmentId2)
    consignmentRefs should contain (consignmentId3)
  }

  "getConsignments" should "return all the consignments after the cursor to the maximum limit where the requested limit is greater than the maximum" in {
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val consignmentId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val consignmentRowParams = List(
      (consignmentId2, "consignment-ref2", 2L),
      (consignmentId3, "consignment-ref3", 3L)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3))

    val limit = 3

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(2, Some("consignment-ref1"))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(2, Some("consignment-ref1")).futureValue

    response.lastCursor should be (Some("consignment-ref3"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain ("consignment-ref2")
    cursors should contain ("consignment-ref3")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain (consignmentId2)
    consignmentRefs should contain (consignmentId3)
  }

  "getConsignments" should "return all the consignments up to the limit where no cursor provided" in {
    val consignmentId1 = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")

    val consignmentRowParams = List(
      (consignmentId1, "consignment-ref1", 1L),
      (consignmentId2, "consignment-ref2", 2L)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, None)).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, None).futureValue

    response.lastCursor should be (Some("consignment-ref2"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain ("consignment-ref1")
    cursors should contain ("consignment-ref2")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain (consignmentId1)
    consignmentRefs should contain (consignmentId2)
  }

  "getConsignments" should "return all the consignments up to the limit where empty cursor provided" in {
    val consignmentId1 = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")

    val consignmentRowParams = List(
      (consignmentId1, "consignment-ref1", 1L),
      (consignmentId2, "consignment-ref2", 2L)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, Some(""))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("")).futureValue

    response.lastCursor should be (Some("consignment-ref2"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain ("consignment-ref1")
    cursors should contain ("consignment-ref2")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain (consignmentId1)
    consignmentRefs should contain (consignmentId2)
  }

  "getConsignments" should "return empty list and no cursor if no consignments" in {
    val limit = 2
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be (None)
    response.consignmentEdges should have size 0
  }

  "getConsignments" should "return empty list and no cursor if limit set to '0'" in {
    val limit = 0
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be (None)
    response.consignmentEdges should have size 0
  }

  "convertToEdges" should "return consignment edges for the given consignment rows" in {
    val consignmentId = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentSeq = 400L
    val consignmentRef = "consignment-ref1"
    val consignmentRow: ConsignmentRow = createConsignmentRow(consignmentId, consignmentRef, consignmentSeq)

    val edges = consignmentService.convertToEdges(Seq(consignmentRow))
    edges.size should be (1)
    val edge = edges.headOption.get
    edge.cursor should equal(consignmentRef)

    val consignment = edge.node
    consignment.consignmentReference should equal(consignmentRef)
    consignment.consignmentid should equal(consignmentId)
    consignment.seriesid should equal(seriesId)
    consignment.userid should equal(userId)
    consignment.transferInitiatedDatetime.get.toInstant should equal(fixedTimeSource)
    consignment.exportDatetime.get.toInstant should equal(fixedTimeSource)
    consignment.createdDateTime.toInstant should equal(fixedTimeSource)
  }

  private def createConsignmentRow(consignmentId: UUID, consignmentRef: String, consignmentSeq: Long): ConsignmentRow = {
    ConsignmentRow(
      consignmentId,
      seriesId,
      userId,
      Timestamp.from(fixedTimeSource),
      None,
      Some(Timestamp.from(fixedTimeSource)),
      None,
      Some(Timestamp.from(fixedTimeSource)),
      None,
      consignmentSeq,
      consignmentRef
    )
  }
}
