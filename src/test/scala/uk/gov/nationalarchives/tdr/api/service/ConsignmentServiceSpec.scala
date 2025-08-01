package uk.gov.nationalarchives.tdr.api.service

import cats.implicits.catsSyntaxOptionId
import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers._
import org.mockito.scalatest.ResetMocksAfterEachTest
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor3
import org.scalatest.prop.Tables.Table
import uk.gov.nationalarchives.Tables.{ConsignmentRow, ConsignmentstatusRow, SeriesRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.model.TransferringBody
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}
import uk.gov.nationalarchives.tdr.keycloak.Token

import java.sql.Timestamp
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentServiceSpec extends AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val fixedTimeSource: Instant = FixedTimeSource.now
  val fixedUuidSource: FixedUUIDSource = new FixedUUIDSource()
  val userId: UUID = UUID.fromString("8d415358-f68b-403b-a90a-daab3fd60109")
  val bodyId: UUID = UUID.fromString("8eae8ed8-201c-11eb-adc1-0242ac120002")
  val bodyName: String = "Mock department"
  val bodyCode: String = "Mock-dept-code-123"
  val bodyDescription: Option[String] = Option("Mock dept description")

  val seriesId: UUID = UUID.fromString("b6b19341-8c33-4272-8636-aafa1e3d98de")
  val consignmentId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val seriesName: String = "Mock series"
  val seriesCode: String = "Mock series"
  val seriesDescription: Option[String] = Option("Series description")

  // scalastyle:off magic.number
  val consignmentSequence: Long = 400L
  // scalastyle:on magic.number
  val consignmentReference = "TDR-2020-VB"
  val mockSeries: SeriesRow = SeriesRow(seriesId, bodyId, seriesName, seriesCode)
  val mockConsignment: ConsignmentRow = ConsignmentRow(
    consignmentId,
    Some(seriesId),
    userId,
    Timestamp.from(FixedTimeSource.now),
    consignmentsequence = consignmentSequence,
    consignmentreference = consignmentReference,
    consignmenttype = "standard",
    bodyid = bodyId,
    seriesname = Some(seriesName),
    transferringbodyname = Some(bodyName),
    transferringbodytdrcode = Some(bodyCode)
  )

  val consignmentRepoMock: ConsignmentRepository = mock[ConsignmentRepository]
  val consignmentStatusRepoMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val transferringBodyServiceMock: TransferringBodyService = mock[TransferringBodyService]
  val seriesRepositoryMock: SeriesRepository = mock[SeriesRepository]
  val mockResponse: Future[ConsignmentRow] = Future.successful(mockConsignment)
  val consignmentService = new ConsignmentService(
    consignmentRepoMock,
    consignmentStatusRepoMock,
    seriesRepositoryMock,
    fileMetadataRepositoryMock,
    transferringBodyServiceMock,
    FixedTimeSource,
    fixedUuidSource,
    ConfigFactory.load()
  )

  "addConsignment" should "create a consignment given correct arguments" in {
    val mockConsignmentSeq = 5L
    val mockToken = mock[Token]
    val mockBody = mock[TransferringBody]
    when(seriesRepositoryMock.getSeries(seriesId)).thenReturn(Future.successful(Seq(mockSeries)))
    when(consignmentRepoMock.getNextConsignmentSequence).thenReturn(Future.successful(mockConsignmentSeq))
    when(consignmentRepoMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)
    when(transferringBodyServiceMock.getBodyByCode(bodyCode)).thenReturn(Future.successful(mockBody))
    when(mockBody.bodyId).thenReturn(bodyId)
    when(mockToken.transferringBody).thenReturn(Some(bodyCode))

    val result = consignmentService.addConsignment(AddConsignmentInput(Some(seriesId), "standard"), mockToken).futureValue

    verify(seriesRepositoryMock).getSeries(seriesId)

    result.consignmentid shouldBe consignmentId
    result.seriesid shouldBe Some(seriesId)
    result.userid shouldBe userId
    result.consignmentType shouldBe "standard"
    result.bodyId shouldBe bodyId
    result.seriesName shouldBe Some(seriesName)
    result.transferringBodyName shouldBe Some(bodyName)
    result.transferringBodyTdrCode shouldBe Some(bodyCode)
  }

  "addConsignment" should "link a consignment to the user's ID" in {
    fixedUuidSource.reset
    val mockToken = mock[Token]
    val mockBody = mock[TransferringBody]
    when(seriesRepositoryMock.getSeries(seriesId)).thenReturn(Future.successful(Seq(mockSeries)))
    when(consignmentRepoMock.getNextConsignmentSequence).thenReturn(Future.successful(consignmentSequence))
    when(consignmentRepoMock.addConsignment(any[ConsignmentRow])).thenReturn(mockResponse)
    when(transferringBodyServiceMock.getBodyByCode(bodyCode)).thenReturn(Future.successful(mockBody))
    when(mockBody.bodyId).thenReturn(bodyId)
    when(mockBody.name).thenReturn(bodyName)
    when(mockBody.tdrCode).thenReturn(bodyCode)
    when(mockToken.transferringBody).thenReturn(Some(bodyCode))
    when(mockToken.userId).thenReturn(userId)
    consignmentService.addConsignment(AddConsignmentInput(Some(seriesId), "standard"), mockToken).futureValue

    verify(consignmentRepoMock).addConsignment(mockConsignment)
  }

  "addConsignment" should "return an error if consignment type input is not recognized" in {
    val mockToken = mock[Token]

    val thrownException = intercept[Exception] {
      consignmentService.addConsignment(AddConsignmentInput(Some(seriesId), "notRecognizedType"), mockToken).futureValue
    }

    thrownException.getMessage should equal("Invalid consignment type 'notRecognizedType' for consignment")
  }

  "addConsignment" should "return an error if the user does not have a body" in {
    val mockToken = mock[Token]
    when(mockToken.userId).thenReturn(userId)
    when(mockToken.transferringBody).thenReturn(None)

    val thrownException = intercept[Exception] {
      consignmentService.addConsignment(AddConsignmentInput(Some(seriesId), "standard"), mockToken).futureValue
    }

    thrownException.getMessage should equal("No transferring body in user token for user '8d415358-f68b-403b-a90a-daab3fd60109'")
  }

  "getConsignment" should "return the specific consignment for the requested consignment id" in {
    val consignmentRow: ConsignmentRow = ConsignmentRow(
      consignmentId,
      Some(seriesId),
      userId,
      Timestamp.from(FixedTimeSource.now),
      exportlocation = Some("Location"),
      consignmentsequence = consignmentSequence,
      consignmentreference = consignmentReference,
      consignmenttype = "standard",
      bodyid = bodyId,
      includetoplevelfolder = Some(true),
      seriesname = Some("seriesName"),
      transferringbodyname = Some("transferringBodyName"),
      transferringbodytdrcode = Some("transferringBodyTdrCode"),
      metadataschemalibraryversion = Some("0.0.Version")
    )
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq(consignmentRow))
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val response: Option[Consignment] = consignmentService.getConsignment(consignmentId).futureValue

    verify(consignmentRepoMock, times(1)).getConsignment(any[UUID])
    val consignment: Consignment = response.get
    consignment.consignmentid should equal(consignmentId)
    consignment.seriesid should equal(Some(seriesId))
    consignment.userid should equal(userId)
    consignment.exportLocation should equal(consignmentRow.exportlocation)
    consignment.includeTopLevelFolder should equal(consignmentRow.includetoplevelfolder)
    consignment.seriesName should equal(consignmentRow.seriesname)
    consignment.transferringBodyName should equal(consignmentRow.transferringbodyname)
    consignment.transferringBodyTdrCode should equal(consignmentRow.transferringbodytdrcode)
    consignment.metadataSchemaLibraryVersion should equal(consignmentRow.metadataschemalibraryversion)
  }

  "getConsignment" should "return none when consignment id does not exist" in {
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignment(any[UUID])).thenReturn(mockResponse)

    val response: Option[Consignment] = consignmentService.getConsignment(UUID.randomUUID()).futureValue
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

  "getConsignmentParentFolder" should "return the parent folder name for a given consignment" in {
    val parentFolder: Option[String] = Option("CONSIGNMENT SERVICE PARENT FOLDER TEST")
    when(consignmentRepoMock.getParentFolder(consignmentId)).thenReturn(Future.successful(parentFolder))

    val parentFolderResult: Option[String] = consignmentService.getConsignmentParentFolder(consignmentId).futureValue
    parentFolderResult shouldBe parentFolder
  }

  "updateExportData" should "update the export data for a given consignment" in {
    val consignmentRepoMock = mock[ConsignmentRepository]
    val seriesRepositoryMock = mock[SeriesRepository]
    val consignmentStatusRepoMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
    val transferringBodyServiceMock: TransferringBodyService = mock[TransferringBodyService]
    val fixedUuidSource = new FixedUUIDSource()

    val service: ConsignmentService = new ConsignmentService(
      consignmentRepoMock,
      consignmentStatusRepoMock,
      seriesRepositoryMock,
      fileMetadataRepositoryMock,
      transferringBodyServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val fixedZonedDatetime = ZonedDateTime.ofInstant(FixedTimeSource.now, ZoneOffset.UTC)
    val consignmentId = UUID.fromString("d8383f9f-c277-49dc-b082-f6e266a39618")
    val input = UpdateExportDataInput(consignmentId, "exportLocation", Some(fixedZonedDatetime), "0.0.Version")
    when(consignmentRepoMock.updateExportData(input)).thenReturn(Future(1))

    val response = service.updateExportData(input).futureValue

    response should be(1)
  }

  "updateTransferInitiated" should "update the transfer initiated fields for a given consignment" in {
    val consignmentRepoMock = mock[ConsignmentRepository]
    val seriesRepositoryMock = mock[SeriesRepository]
    val transferringBodyServiceMock: TransferringBodyService = mock[TransferringBodyService]
    val fixedUuidSource = new FixedUUIDSource()
    val consignmentStatusCaptor: ArgumentCaptor[ConsignmentstatusRow] = ArgumentCaptor.forClass(classOf[ConsignmentstatusRow])

    val service: ConsignmentService = new ConsignmentService(
      consignmentRepoMock,
      consignmentStatusRepoMock,
      seriesRepositoryMock,
      fileMetadataRepositoryMock,
      transferringBodyServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val consignmentId = UUID.fromString("d8383f9f-c277-49dc-b082-f6e266a39618")
    val userId = UUID.randomUUID()
    val now = Timestamp.from(FixedTimeSource.now)
    val consignmentStatusId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val expectedConsignmentStatusRow: ConsignmentstatusRow = ConsignmentstatusRow(consignmentStatusId, consignmentId, "Export", "InProgress", now)

    when(consignmentRepoMock.updateTransferInitiated(consignmentId, userId, now)).thenReturn(Future(1))
    when(consignmentStatusRepoMock.addConsignmentStatus(consignmentStatusCaptor.capture())).thenReturn(Future(expectedConsignmentStatusRow))

    val response = service.updateTransferInitiated(consignmentId, userId).futureValue

    val actualConsignmentStatusRow = consignmentStatusCaptor.getValue
    actualConsignmentStatusRow should equal(expectedConsignmentStatusRow)
    response should be(1)
  }

  "updateSeriesOfConsignment" should "update the seriesId, seriesName and status for a given consignment" in {
    val updateConsignmentSeriesIdInput = UpdateConsignmentSeriesIdInput(consignmentId, seriesId)
    val statusType = "Series"
    val expectedSeriesStatus = Completed
    val expectedResult = 1
    when(consignmentRepoMock.updateSeriesOfConsignment(updateConsignmentSeriesIdInput, Some(seriesName)))
      .thenReturn(Future.successful(1))
    when(consignmentStatusRepoMock.updateConsignmentStatus(consignmentId, statusType, Completed, Timestamp.from(fixedTimeSource)))
      .thenReturn(Future.successful(1))
    when(seriesRepositoryMock.getSeries(updateConsignmentSeriesIdInput.seriesId)).thenReturn(Future.successful(Seq(mockSeries)))

    val result = consignmentService.updateSeriesOfConsignment(updateConsignmentSeriesIdInput).futureValue

    verify(consignmentRepoMock).updateSeriesOfConsignment(updateConsignmentSeriesIdInput, Some(seriesName))
    verify(consignmentStatusRepoMock)
      .updateConsignmentStatus(updateConsignmentSeriesIdInput.consignmentId, statusType, expectedSeriesStatus, Timestamp.from(fixedTimeSource))

    result should equal(expectedResult)
  }

  "updateSeriesOfConsignment" should "update the status with 'Failed' if seriesId update fails for a given consignment" in {
    val updateConsignmentSeriesIdInput = UpdateConsignmentSeriesIdInput(consignmentId, seriesId)
    val statusType = "Series"
    val expectedSeriesStatus = Failed
    val expectedResult = 0
    when(consignmentRepoMock.updateSeriesOfConsignment(updateConsignmentSeriesIdInput, Some(seriesName)))
      .thenReturn(Future.successful(0))
    when(consignmentStatusRepoMock.updateConsignmentStatus(consignmentId, statusType, Failed, Timestamp.from(fixedTimeSource)))
      .thenReturn(Future.successful(1))
    when(seriesRepositoryMock.getSeries(updateConsignmentSeriesIdInput.seriesId)).thenReturn(Future.successful(Seq(mockSeries)))

    val result = consignmentService.updateSeriesOfConsignment(updateConsignmentSeriesIdInput).futureValue

    verify(consignmentRepoMock).updateSeriesOfConsignment(updateConsignmentSeriesIdInput, Some(seriesName))
    verify(consignmentStatusRepoMock)
      .updateConsignmentStatus(updateConsignmentSeriesIdInput.consignmentId, statusType, expectedSeriesStatus, Timestamp.from(fixedTimeSource))

    result should equal(expectedResult)
  }

  "getConsignments" should "return all the consignments after the cursor to the limit" in {
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val consignmentId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")
    val exportLocation2 = Some("Location2")
    val exportLocation3 = Some("Location3")

    val consignmentRowParams = List(
      (consignmentId2, "consignment-ref2", 2L, exportLocation2),
      (consignmentId3, "consignment-ref3", 3L, exportLocation3)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3, p._4))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be(Some("consignment-ref3"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain("consignment-ref2")
    cursors should contain("consignment-ref3")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain(consignmentId2)
    consignmentRefs should contain(consignmentId3)

    val exportLocations: List[Option[String]] = edges.map(e => e.node.exportLocation).toList
    exportLocations should contain(exportLocation2)
    exportLocations should contain(exportLocation3)
  }

  "getConsignments" should "return all the consignments after the cursor to the maximum limit where the requested limit is greater than the maximum" in {
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val consignmentId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")
    val exportLocation2 = Some("Location2")
    val exportLocation3 = Some("Location3")

    val consignmentRowParams = List(
      (consignmentId2, "consignment-ref2", 2L, exportLocation2),
      (consignmentId3, "consignment-ref3", 3L, exportLocation3)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3, p._4))

    val limit = 3

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(2, Some("consignment-ref1"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be(Some("consignment-ref3"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain("consignment-ref2")
    cursors should contain("consignment-ref3")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain(consignmentId2)
    consignmentRefs should contain(consignmentId3)

    val exportLocations: List[Option[String]] = edges.map(e => e.node.exportLocation).toList
    exportLocations should contain(exportLocation2)
    exportLocations should contain(exportLocation3)
  }

  "getConsignments" should "return all the consignments up to the limit where no cursor provided" in {
    val consignmentId1 = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val exportLocation1 = Some("Location2")
    val exportLocation2 = Some("Location3")

    val consignmentRowParams = List(
      (consignmentId1, "consignment-ref1", 2L, exportLocation1),
      (consignmentId2, "consignment-ref2", 3L, exportLocation2)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3, p._4))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, None, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, None).futureValue

    response.lastCursor should be(Some("consignment-ref2"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain("consignment-ref1")
    cursors should contain("consignment-ref2")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain(consignmentId1)
    consignmentRefs should contain(consignmentId2)

    val exportLocations: List[Option[String]] = edges.map(e => e.node.exportLocation).toList
    exportLocations should contain(exportLocation1)
    exportLocations should contain(exportLocation2)
  }

  "getConsignments" should "return all the consignments up to the limit where empty cursor provided" in {
    val consignmentId1 = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val exportLocation1 = Some("Location2")
    val exportLocation2 = Some("Location3")

    val consignmentRowParams = List(
      (consignmentId1, "consignment-ref1", 2L, exportLocation1),
      (consignmentId2, "consignment-ref2", 3L, exportLocation2)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3, p._4))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, Some(""), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("")).futureValue

    response.lastCursor should be(Some("consignment-ref2"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain("consignment-ref1")
    cursors should contain("consignment-ref2")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain(consignmentId1)
    consignmentRefs should contain(consignmentId2)

    val exportLocations: List[Option[String]] = edges.map(e => e.node.exportLocation).toList
    exportLocations should contain(exportLocation1)
    exportLocations should contain(exportLocation2)
  }

  "getConsignments" should "return filtered consignments when currentPage and consignment filter are passed" in {
    val consignmentId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val consignmentId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")
    val exportLocation2 = Some("Location2")
    val exportLocation3 = Some("Location3")

    val consignmentRowParams = List(
      (consignmentId2, "consignment-ref2", 2L, exportLocation2),
      (consignmentId3, "consignment-ref3", 3L, exportLocation3)
    )

    val consignmentRows: List[ConsignmentRow] = consignmentRowParams.map(p => createConsignmentRow(p._1, p._2, p._3, p._4))

    val limit = 2

    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(consignmentRows)
    when(consignmentRepoMock.getConsignments(limit, None, 2.some, ConsignmentFilters(userId.some, None).some, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)))
      .thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, None, ConsignmentFilters(userId.some, None).some, 2.some).futureValue

    response.lastCursor should be(Some("consignment-ref3"))
    response.consignmentEdges should have size 2
    val edges = response.consignmentEdges
    val cursors: List[String] = edges.map(e => e.cursor).toList
    cursors should contain("consignment-ref2")
    cursors should contain("consignment-ref3")

    val consignmentRefs: List[UUID] = edges.map(e => e.node.consignmentid).toList
    consignmentRefs should contain(consignmentId2)
    consignmentRefs should contain(consignmentId3)

    val exportLocations: List[Option[String]] = edges.map(e => e.node.exportLocation).toList
    exportLocations should contain(exportLocation2)
    exportLocations should contain(exportLocation3)
  }

  "getConsignments" should "return empty list and no cursor if no consignments" in {
    val limit = 2
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be(None)
    response.consignmentEdges should have size 0
  }

  "getConsignments" should "return empty list and no cursor if limit set to '0'" in {
    val limit = 0
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignments(limit, Some("consignment-ref1"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))).thenReturn(mockResponse)

    val response: PaginatedConsignments = consignmentService.getConsignments(limit, Some("consignment-ref1")).futureValue

    response.lastCursor should be(None)
    response.consignmentEdges should have size 0
  }

  val totalPagesTable: TableFor3[Int, Int, Int] = Table(
    ("limit", "totalConsignments", "totalPages"),
    (1, 3, 3),
    (4, 5, 3),
    (5, 3, 2),
    (2, 6, 3)
  )

  forAll(totalPagesTable) { (limit, totalConsignments, totalPages) =>
    "getTotalPages" should s"return total pages as $totalPages when the limit is $limit and totalConsignments are $totalConsignments" in {
      val consignmentFilters = Some(ConsignmentFilters(userId.some, None))
      when(consignmentRepoMock.getTotalConsignments(consignmentFilters)).thenReturn(Future.successful(totalConsignments))

      val response = consignmentService.getTotalPages(limit, consignmentFilters).futureValue

      response should be(totalPages)
    }
  }

  "getConsignmentsForMetadataReview" should "return a list of consignments" in {
    val consignmentRow: ConsignmentRow = ConsignmentRow(
      consignmentId,
      Some(seriesId),
      userId,
      Timestamp.from(FixedTimeSource.now),
      exportlocation = Some("Location"),
      consignmentsequence = consignmentSequence,
      consignmentreference = consignmentReference,
      consignmenttype = "standard",
      bodyid = bodyId,
      includetoplevelfolder = Some(true),
      seriesname = Some("seriesName"),
      transferringbodyname = Some("transferringBodyName"),
      transferringbodytdrcode = Some("transferringBodyTdrCode")
    )
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq(consignmentRow))
    when(consignmentRepoMock.getConsignmentsForMetadataReview).thenReturn(mockResponse)

    val response: Seq[Consignment] = consignmentService.getConsignmentsForMetadataReview.futureValue

    verify(consignmentRepoMock, times(1)).getConsignmentsForMetadataReview
    val consignment: Consignment = response.head
    consignment.consignmentid should equal(consignmentId)
    consignment.seriesid should equal(Some(seriesId))
    consignment.userid should equal(userId)
    consignment.seriesName should equal(consignmentRow.seriesname)
    consignment.transferringBodyName should equal(consignmentRow.transferringbodyname)
    consignment.transferringBodyTdrCode should equal(consignmentRow.transferringbodytdrcode)
  }

  "getConsignmentForMetadataReview" should "return a given consignment" in {
    val consignmentRow: ConsignmentRow = ConsignmentRow(
      consignmentId,
      Some(seriesId),
      userId,
      Timestamp.from(FixedTimeSource.now),
      exportlocation = Some("Location"),
      consignmentsequence = consignmentSequence,
      consignmentreference = consignmentReference,
      consignmenttype = "standard",
      bodyid = bodyId,
      includetoplevelfolder = Some(true),
      seriesname = Some("seriesName"),
      transferringbodyname = Some("transferringBodyName"),
      transferringbodytdrcode = Some("transferringBodyTdrCode")
    )
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq(consignmentRow))
    when(consignmentRepoMock.getConsignmentForMetadataReview(consignmentId)).thenReturn(mockResponse)

    val response: Option[Consignment] = consignmentService.getConsignmentForMetadataReview(consignmentId).futureValue

    val consignment: Consignment = response.get
    consignment.consignmentid should equal(consignmentId)
    consignment.seriesid should equal(Some(seriesId))
    consignment.userid should equal(userId)
    consignment.seriesName should equal(consignmentRow.seriesname)
    consignment.transferringBodyName should equal(consignmentRow.transferringbodyname)
    consignment.transferringBodyTdrCode should equal(consignmentRow.transferringbodytdrcode)
  }

  "getConsignmentForMetadataReview" should "not return any consignment if the consignment doesn't exist" in {
    val mockResponse: Future[Seq[ConsignmentRow]] = Future.successful(Seq())
    when(consignmentRepoMock.getConsignmentForMetadataReview(consignmentId)).thenReturn(mockResponse)

    val response: Option[Consignment] = consignmentService.getConsignmentForMetadataReview(consignmentId).futureValue
    response should equal(None)
  }

  "startUpload" should "create an upload in progress status, add the parent folder and 'IncludeTopLevelFolder'" in {
    val startUploadInputCaptor: ArgumentCaptor[StartUploadInput] = ArgumentCaptor.forClass(classOf[StartUploadInput])
    val consignmentStatusCaptor: ArgumentCaptor[List[ConsignmentstatusRow]] = ArgumentCaptor.forClass(classOf[List[ConsignmentstatusRow]])
    val parentFolder = "parentFolder"

    val startUploadInput = StartUploadInput(consignmentId, parentFolder, true)
    when(consignmentStatusRepoMock.getConsignmentStatus(any[UUID])).thenReturn(Future(Seq()))
    when(consignmentRepoMock.addUploadDetails(startUploadInputCaptor.capture(), consignmentStatusCaptor.capture())(any[ExecutionContext]))
      .thenReturn(Future.successful(parentFolder))
    consignmentService.startUpload(startUploadInput).futureValue

    startUploadInputCaptor.getValue should be(startUploadInput)

    val statusRow = consignmentStatusCaptor.getValue.find(_.statustype == "Upload").get
    statusRow.consignmentid should be(consignmentId)
    statusRow.statustype should be("Upload")
    statusRow.value should be("InProgress")
  }

  "startUpload" should "create a ClientChecks in progress status" in {
    val consignmentStatusCaptor: ArgumentCaptor[List[ConsignmentstatusRow]] = ArgumentCaptor.forClass(classOf[List[ConsignmentstatusRow]])
    val startUploadInputCaptor: ArgumentCaptor[StartUploadInput] = ArgumentCaptor.forClass(classOf[StartUploadInput])
    val parentFolder = "parentFolder"
    val startUploadInput = StartUploadInput(consignmentId, parentFolder, false)

    when(consignmentStatusRepoMock.getConsignmentStatus(any[UUID])).thenReturn(Future(Seq()))
    when(consignmentRepoMock.addUploadDetails(startUploadInputCaptor.capture(), consignmentStatusCaptor.capture())(any[ExecutionContext]))
      .thenReturn(Future.successful(parentFolder))
    consignmentService.startUpload(startUploadInput).futureValue

    startUploadInputCaptor.getValue should be(startUploadInput)

    val statusRow = consignmentStatusCaptor.getValue.find(_.statustype == "ClientChecks").get
    statusRow.consignmentid should be(consignmentId)
    statusRow.statustype should be("ClientChecks")
    statusRow.value should be("InProgress")
  }

  "startUpload" should "return an error if there is an existing consignment status" in {
    val statusRows = Seq(ConsignmentstatusRow(UUID.randomUUID(), consignmentId, "Upload", "InProgress", Timestamp.from(FixedTimeSource.now), Option.empty))
    when(consignmentStatusRepoMock.getConsignmentStatus(any[UUID])).thenReturn(Future(statusRows))
    val exception = consignmentService.startUpload(StartUploadInput(consignmentId, "parentFolder", false)).failed.futureValue
    exception.getMessage should equal("Existing consignment upload status is 'InProgress', so cannot start new upload")
  }

  "updateMetadataSchemaLibraryVersionOfConsignment" should "pass correct input values to repository" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val schemaVersion = "12344"
    val inputCaptor: ArgumentCaptor[UpdateMetadataSchemaLibraryVersionInput] = ArgumentCaptor.forClass(classOf[UpdateMetadataSchemaLibraryVersionInput])

    when(consignmentRepoMock.updateMetadataSchemaLibraryVersion(inputCaptor.capture()))
      .thenReturn(Future.successful(1))
    consignmentService.updateMetadataSchemaLibraryVersion(UpdateMetadataSchemaLibraryVersionInput(consignmentId, schemaVersion))
    inputCaptor.getValue.consignmentId should equal(consignmentId)
    inputCaptor.getValue.metadataSchemaLibraryVersion should equal(schemaVersion)
  }

  "updateClientSideDraftMetadataFileName" should "pass correct input values to repository" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val clientSideDraftMetadataFileName = "some file name.csv"
    val inputCaptor: ArgumentCaptor[UpdateClientSideDraftMetadataFileNameInput] = ArgumentCaptor.forClass(classOf[UpdateClientSideDraftMetadataFileNameInput])

    when(consignmentRepoMock.updateClientSideDraftMetadataFileName(inputCaptor.capture()))
      .thenReturn(Future.successful(1))

    consignmentService.updateClientSideDraftMetadataFileName(UpdateClientSideDraftMetadataFileNameInput(consignmentId, clientSideDraftMetadataFileName)).futureValue
    inputCaptor.getValue.consignmentId should equal(consignmentId)
    inputCaptor.getValue.clientSideDraftMetadataFileName should equal(clientSideDraftMetadataFileName)
  }

  "getConsignments" should "set cursor in ConsignmentEdge based on consignmentReference when ordering by reference" in {
    val consignmentId1 = UUID.randomUUID()
    val consignmentId2 = UUID.randomUUID()
    val consignmentRows = List(
      createConsignmentRow(consignmentId1, "ref-a", 1L, None),
      createConsignmentRow(consignmentId2, "ref-b", 2L, None)
    )

    val orderBy = ConsignmentOrderBy(ConsignmentReference, Ascending)
    when(consignmentRepoMock.getConsignments(2, None, None, None, orderBy))
      .thenReturn(Future.successful(consignmentRows))

    val response = consignmentService.getConsignments(2, None, None, None, Some(orderBy)).futureValue

    val edges = response.consignmentEdges
    edges(0).cursor should be("ref-a")
    edges(1).cursor should be("ref-b")
    response.lastCursor should be(Some("ref-b"))
  }

  "getConsignments" should "set cursor in ConsignmentEdge based on timestamp when ordering by createdDatetime" in {
    val consignmentId1 = UUID.randomUUID()
    val consignmentId2 = UUID.randomUUID()
    val timestamp1 = Timestamp.from(Instant.parse("2024-01-01T10:00:00Z"))
    val timestamp2 = Timestamp.from(Instant.parse("2024-01-02T10:00:00Z"))

    val consignmentRow1 = ConsignmentRow(
      consignmentid = consignmentId1,
      seriesid = Some(seriesId),
      userid = userId,
      datetime = timestamp1,
      parentfolder = None,
      transferinitiateddatetime = Some(timestamp1),
      transferinitiatedby = None,
      exportdatetime = Some(timestamp1),
      exportlocation = None,
      consignmentsequence = 1L,
      consignmentreference = "ref-a",
      consignmenttype = "standard",
      bodyid = bodyId
    )

    val consignmentRow2 = ConsignmentRow(
      consignmentid = consignmentId2,
      seriesid = Some(seriesId),
      userid = userId,
      datetime = timestamp2,
      parentfolder = None,
      transferinitiateddatetime = Some(timestamp2),
      transferinitiatedby = None,
      exportdatetime = Some(timestamp2),
      exportlocation = None,
      consignmentsequence = 2L,
      consignmentreference = "ref-b",
      consignmenttype = "standard",
      bodyid = bodyId
    )

    val orderBy = ConsignmentOrderBy(CreatedAtTimestamp, Ascending)
    when(consignmentRepoMock.getConsignments(2, None, None, None, orderBy))
      .thenReturn(Future.successful(List(consignmentRow1, consignmentRow2)))

    val response = consignmentService.getConsignments(2, None, None, None, Some(orderBy)).futureValue

    val edges = response.consignmentEdges
    edges(0).cursor should be(timestamp1.toString)
    edges(1).cursor should be(timestamp2.toString)
    response.lastCursor should be(Some(timestamp2.toString))
  }

  "totalClosedRecords" should "return total number of closed records" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    when(fileMetadataRepositoryMock.totalClosedRecords(consignmentId))
      .thenReturn(Future.successful(5))

    val closedRecords = consignmentService.totalClosedRecords(consignmentId).futureValue
    closedRecords should equal(5)
  }

  private def createConsignmentRow(consignmentId: UUID, consignmentRef: String, consignmentSeq: Long, exportLocation: Option[String]) = {
    ConsignmentRow(
      consignmentId,
      Some(seriesId),
      userId,
      Timestamp.from(fixedTimeSource),
      None,
      Some(Timestamp.from(fixedTimeSource)),
      None,
      Some(Timestamp.from(fixedTimeSource)),
      exportLocation,
      consignmentSeq,
      consignmentRef,
      "standard",
      bodyId
    )
  }
}
