package uk.gov.nationalarchives.tdr.api.service

import cats.implicits.catsSyntaxOptionId
import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers._
import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks}
import uk.gov.nationalarchives.Tables.{
  ConsignmentRow,
  FfidmetadataRow,
  FfidmetadatamatchesRow,
  FileRow,
  FilemetadataRow,
  FilepropertyvaluesRow,
  FilestatusRow
}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.QueriedFileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.PaginationInput
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, AllDescendantsInput, ClientSideMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.staticMetadataProperties
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

//scalastyle:off file.size.limit
class FileServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  private val bodyId = UUID.randomUUID()
  val uuidSource: FixedUUIDSource = new FixedUUIDSource()

  val consignmentRepositoryMock: ConsignmentRepository = mock[ConsignmentRepository]
  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val customMetadataPropertiesRepositoryMock: CustomMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
  val customMetadataServiceMock: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val antivirusMetadataRepositoryMock: AntivirusMetadataRepository = mock[AntivirusMetadataRepository]
  val validateFileMetadataServiceMock: ValidateFileMetadataService = mock[ValidateFileMetadataService]
  val consignmentStatusService = new ConsignmentStatusService(consignmentStatusRepositoryMock, fileStatusRepositoryMock, uuidSource, FixedTimeSource)
  val fileMetadataService =
    new FileMetadataService(
      fileMetadataRepositoryMock,
      fileRepositoryMock,
      consignmentStatusService,
      customMetadataServiceMock,
      validateFileMetadataServiceMock,
      FixedTimeSource,
      uuidSource
    )
  val queriedFileFieldsWithoutOriginalPath: QueriedFileFields = QueriedFileFields()

  // scalastyle:off magic.number
  "getOwnersOfFiles" should "find the owners of the given files" in {
    val fixedUuidSource = new FixedUUIDSource()
    val fileId1 = UUID.fromString("bc609dc4-e153-4620-a7ab-20e7fd5a4005")
    val fileId2 = UUID.fromString("67178a08-36ea-41c2-83ee-4b343b6429cb")
    val userId1 = UUID.fromString("e9cac50f-c5eb-42b4-bb5d-355ccf8920cc")
    val userId2 = UUID.fromString("f4ffe1d0-3525-4a7c-ba0c-812f6e054ab1")
    val seriesId1 = UUID.fromString("bb503ea6-7207-42d7-9844-81471aa1b36a")
    val seriesId2 = UUID.fromString("74394d89-aa22-4170-b50e-3f5eefda7062")
    val consignmentId1 = UUID.fromString("0ae52efa-4f01-4b05-84f1-e36626180dad")
    val consignmentId2 = UUID.fromString("2e29cc1c-0a3e-40b2-b39d-f60bfea88abe")

    val consignment1 = ConsignmentRow(
      consignmentId1,
      Some(seriesId1),
      userId1,
      Timestamp.from(Instant.now),
      consignmentsequence = 400L,
      consignmentreference = "TEST-TDR-2021-VB",
      consignmenttype = "standard",
      bodyid = bodyId
    )
    val consignment2 = ConsignmentRow(
      consignmentId2,
      Some(seriesId2),
      userId2,
      Timestamp.from(Instant.now),
      consignmentsequence = 500L,
      consignmentreference = "TEST-TDR-2021-3B",
      consignmenttype = "standard",
      bodyid = bodyId
    )

    val fileMetadataService =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        fileRepositoryMock,
        consignmentStatusService,
        customMetadataServiceMock,
        validateFileMetadataServiceMock,
        FixedTimeSource,
        fixedUuidSource
      )
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock, fixedUuidSource)

    val fileService = new FileService(
      fileRepositoryMock,
      consignmentRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    when(consignmentRepositoryMock.getConsignmentsOfFiles(Seq(fileId1)))
      .thenReturn(Future.successful(Seq((fileId1, consignment1), (fileId2, consignment2))))
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileId1, "value", Timestamp.from(Instant.now), userId1, "name")))
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[FilemetadataRow]])).thenReturn(mockFileMetadataResponse)

    val owners = fileService.getOwnersOfFiles(Seq(fileId1)).futureValue

    owners should have size 2

    owners.head.userId should equal(userId1)
    owners(1).userId should equal(userId2)
  }
  // scalastyle:on magic.number


  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are greater than batch size" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileMetadataService =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        fileRepositoryMock,
        consignmentStatusService,
        customMetadataServiceMock,
        validateFileMetadataServiceMock,
        FixedTimeSource,
        new FixedUUIDSource()
      )
    val fixedUuidSource = new FixedUUIDSource()
    val fileStatusServiceMock = mock[FileStatusService]

    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val file1Id = UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f")
    val file2Id = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/nested/path/OriginalPath1", "Checksum1", 1L, 1L, 1)
    val metadataInputTwo = ClientSideMetadataInput("OriginalPath2", "Checksum2", 1L, 1L, 2)

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    when(fileStatusServiceMock.addFileStatuses(any[AddMultipleFileStatusesInput])).thenReturn(Future(Nil))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      consignmentRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusServiceMock,
      fileMetadataService,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val expectedStatusInput = AddMultipleFileStatusesInput(
      List(
        AddFileStatusInput(file1Id, "ClosureMetadata", "NotEntered"),
        AddFileStatusInput(file1Id, "DescriptiveMetadata", "NotEntered"),
        AddFileStatusInput(file2Id, "ClosureMetadata", "NotEntered"),
        AddFileStatusInput(file2Id, "DescriptiveMetadata", "NotEntered")
      )
    )

    val response = service.addFile(AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo)), userId).futureValue

    verify(fileRepositoryMock, times(2)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())
    verify(fileStatusServiceMock, times(1)).addFileStatuses(expectedStatusInput)

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(file1Id)
    response.head.matchId should equal(2)

    response.last.fileId should equal(file2Id)
    response.last.matchId should equal(1)

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 46
    metadataRows.size should equal(expectedSize)
    staticMetadataProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop.name && r.value == prop.value) should equal(5)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType => count should equal(5) // Directories have this set
        case _                                                => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  def checkFileStatusRows(rows: List[FilestatusRow], response: List[FileMatches], statusType: String, firstStatus: String, secondStatus: String): Unit = {

    rows.head.filestatusid != null shouldBe true
    rows.head.fileid should equal(response.head.fileId)
    rows.head.statustype should equal(statusType)
    rows.head.value should equal(firstStatus)
    rows.head.createddatetime != null shouldBe true

    rows.last.filestatusid != null shouldBe true
    rows.last.fileid should equal(response.last.fileId)
    rows.last.statustype should equal(statusType)
    rows.last.value should equal(secondStatus)
    rows.last.createddatetime != null shouldBe true
  }

  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are less than batch size" in {
    val consignmentId = UUID.randomUUID()
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val fileMetadataService =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        fileRepositoryMock,
        consignmentStatusService,
        customMetadataServiceMock,
        validateFileMetadataServiceMock,
        FixedTimeSource,
        new FixedUUIDSource()
      )
    val fixedUuidSource = new FixedUUIDSource()
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock, fixedUuidSource)

    val userId = UUID.randomUUID()

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/OriginalPath1", "Checksum1", 1L, 1L, 1)
    val metadataInputTwo = ClientSideMetadataInput("", "", 1L, 1L, 2)

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      consignmentRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val response = service.addFile(AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo)), userId).futureValue

    verify(fileRepositoryMock, times(1)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.head.matchId should equal(1)

    response.last.fileId should equal(UUID.fromString("8e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.last.matchId should equal(2)

    val expectedFileRows = 3
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 30
    metadataRows.size should equal(expectedSize)
    staticMetadataProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop.name && r.value == prop.value) should equal(3)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType => count should equal(3) // Directories have this set
        case _                                                => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "getPaginatedFiles" should "return all the file edges after the cursor to the limit" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))
    val limit = 2
    val page = 0
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, page, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[FileMetadataService.File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2
    firstEdge.node.parentId.get shouldBe parentId
    firstEdge.node.fileType.get shouldBe NodeType.fileTypeIdentifier
    firstEdge.node.fileName.get shouldBe "fileName2"
    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
    secondEdge.node.parentId.get shouldBe parentId
    secondEdge.node.fileType.get shouldBe NodeType.fileTypeIdentifier
    secondEdge.node.fileName.get shouldBe "fileName3"
  }

  "getPaginatedFiles" should "return all the files edges after the cursor to the maximum limit where the requested limit is greater than the maximum" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limitExceedingMax = 3
    val expectedMaxLimit = 2
    val currentPage = 0
    val offset = 0
    val input = Some(PaginationInput(Some(limitExceedingMax), Some(currentPage), Some(fileId1), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, expectedMaxLimit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return all the file edges up to the limit where no cursor provided" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limit = 2
    val page = 0
    val offset = 0
    val input = Some(PaginationInput(Some(limit), Some(page), None, None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, None, FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe false

    edges.size shouldBe 2
    response.totalItems shouldBe 2

    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return all the file edges up to the limit where filters provided" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limit = 2
    val page = 0
    val offset = 0
    val fileFilters = FileFilters(Some(NodeType.fileTypeIdentifier))
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), Some(fileFilters)))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, fileFilters.fileTypeIdentifier))
      .thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), fileFilters))
      .thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue
    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    response.totalItems shouldBe 2

    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return no files edges if no files exist" in {
    val consignmentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileRepositoryMock = mock[FileRepository]
    val limit = 2
    val page = 0
    val offset = 0
    val mockResponse: Future[Seq[FileRow]] = Future.successful(Seq())
    val selectedFileIds: Option[Set[UUID]] = Some(Set())

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(0))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), None))

    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor shouldBe None
    pageInfo.endCursor shouldBe None
    pageInfo.hasNextPage shouldBe false
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 0
    response.totalItems shouldBe 0
  }

  "getPaginatedFiles" should "return an error if no pagination input argument provided" in {
    val consignmentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileRepositoryMock = mock[FileRepository]
    val limit = 2
    val offset = 0
    val mockResponse: Future[Seq[FileRow]] = Future.successful(Seq())
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val thrownException = intercept[Exception] {
      fileService.getPaginatedFiles(consignmentId, None, queriedFileFieldsWithoutOriginalPath).futureValue
    }

    thrownException.getMessage should equal("No pagination input argument provided for 'paginatedFiles' field query")
  }

  "getPaginatedFiles" should "return all the files by natural sorting order" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()

    val fileRowParams = List(
      (UUID.randomUUID(), consignmentId, "fileName2", parentId),
      (UUID.randomUUID(), consignmentId, "fileName22", parentId),
      (UUID.randomUUID(), consignmentId, "fileName21", parentId),
      (UUID.randomUUID(), consignmentId, "fileName31", parentId),
      (UUID.randomUUID(), consignmentId, "fileName32", parentId),
      (UUID.randomUUID(), consignmentId, "fileName1", parentId),
      (UUID.randomUUID(), consignmentId, "fileName5", parentId),
      (UUID.randomUUID(), consignmentId, "fileName", parentId),
      (UUID.randomUUID(), consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))
    val limit = 100
    val page = 0
    val input = Some(PaginationInput(Some(limit), Some(page), Some(parentId.toString), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)

    when(fileMetadataRepositoryMock.getFileMetadata(ArgumentMatchers.eq(consignmentId), any[Option[Set[UUID]]], any[Option[Set[String]]]))
      .thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(ArgumentMatchers.eq(consignmentId), any[Option[Set[UUID]]]())).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(ArgumentMatchers.eq(consignmentId), any())).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(ArgumentMatchers.eq(consignmentId), ArgumentMatchers.eq(Set(FFID)), any())).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(ArgumentMatchers.eq(consignmentId), any(), any())).thenReturn(Future.successful(8))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, 2, page, Some(parentId.toString), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val edges = response.edges

    edges.size shouldBe fileRowParams.size
    edges.map(_.node.fileName.getOrElse("")) should equal(
      List("fileName", "fileName1", "fileName2", "fileName3", "fileName5", "fileName21", "fileName22", "fileName31", "fileName32")
    )
  }

  "getConsignmentParentFolderId" should "return the parent folder id for a given consignment" in {
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val parentFolderRow = FileRow(parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))

    val fileService = setupFileService(fileRepositoryMock)
    when(fileRepositoryMock.getConsignmentParentFolder(consignmentId)).thenReturn(Future.successful(Seq(parentFolderRow)))

    val parentFolderIdResult: Option[UUID] = fileService.getConsignmentParentFolderId(consignmentId).futureValue
    parentFolderIdResult.get shouldBe parentFolderId
  }

  "getConsignmentParentFolderId" should "return None if the parent folder does not exist for a given consignment" in {
    val consignmentId = UUID.randomUUID()
    when(fileRepositoryMock.getConsignmentParentFolder(consignmentId)).thenReturn(Future.successful(Seq()))

    val fileService = setupFileService(fileRepositoryMock)
    val parentFolderIdResult: Option[UUID] = fileService.getConsignmentParentFolderId(consignmentId).futureValue
    parentFolderIdResult shouldBe None
  }

  "getAllDescendants" should "return all the descendants for the given parent ids" in {
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()

    val fileId1 = UUID.fromString("bc609dc4-e153-4620-a7ab-20e7fd5a4005")
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")

    val fileRowParams = List(
      (fileId1, consignmentId, "fileName1", parentId),
      (fileId2, consignmentId, "fileName2", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    when(fileRepositoryMock.getAllDescendants(List(parentId))).thenReturn(Future.successful(fileRows))
    val fileService = setupFileService(fileRepositoryMock)

    val response = fileService.getAllDescendants(AllDescendantsInput(consignmentId, List(parentId))).futureValue
    response.size shouldBe 2
    val firstFile = response.head
    firstFile.fileId shouldBe fileId1
    firstFile.fileName shouldBe Some("fileName1")
    firstFile.fileType shouldBe Some("File")
    firstFile.parentId shouldBe Some(parentId)
    firstFile.metadata shouldEqual FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    firstFile.antivirusMetadata shouldBe None
    firstFile.ffidMetadata shouldBe None

    val secondFile = response.last
    secondFile.fileId shouldBe fileId2
    secondFile.fileName shouldBe Some("fileName2")
    secondFile.fileType shouldBe Some("File")
    secondFile.parentId shouldBe Some(parentId)
    secondFile.metadata shouldEqual FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    secondFile.antivirusMetadata shouldBe None
    secondFile.ffidMetadata shouldBe None
  }

  "getAllDescendants" should "return an empty list if no descendants found" in {
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()

    when(fileRepositoryMock.getAllDescendants(List(parentId))).thenReturn(Future.successful(List()))
    val fileService = setupFileService(fileRepositoryMock)

    val response = fileService.getAllDescendants(AllDescendantsInput(consignmentId, List(parentId))).futureValue
    response.size shouldBe 0
  }

  private def setupFileService(fileRepositoryMock: FileRepository): FileService = {
    val fixedUuidSource = new FixedUUIDSource()
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock, fixedUuidSource)

    new FileService(
      fileRepositoryMock,
      consignmentRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )
  }

  private def ffidMetadataRow(ffidMetadataid: UUID, fileId: UUID, datetime: Timestamp): FfidmetadataRow =
    FfidmetadataRow(ffidMetadataid, fileId, "pronom", "1.0", datetime, "signaturefileversion", "signature", "pronom")

  private def ffidMetadataMatchesRow(ffidMetadataid: UUID): FfidmetadatamatchesRow =
    FfidmetadatamatchesRow(ffidMetadataid, Some("txt"), "identification", Some("x-fmt/111"))

  private def fileMetadataRow(fileId: UUID, propertyName: String, value: String): FilemetadataRow =
    FilemetadataRow(UUID.randomUUID(), fileId, value, Timestamp.from(Instant.now()), UUID.randomUUID(), propertyName)

  private def createFileRow(id: UUID, consignmentId: UUID, fileName: String, parentId: UUID): FileRow = {
    FileRow(id, consignmentId, userId, Timestamp.from(FixedTimeSource.now), Some(true), Some(NodeType.fileTypeIdentifier), Some(fileName), Some(parentId))
  }

  private def mockCustomMetadataValuesResponse(customMetadataMock: CustomMetadataPropertiesRepository): ScalaOngoingStubbing[Future[Seq[FilepropertyvaluesRow]]] = {
    val staticMetadataRows = staticMetadataProperties.map(staticMetadata => {
      FilepropertyvaluesRow(staticMetadata.name, staticMetadata.value, Some(true))
    })

    when(customMetadataMock.getCustomMetadataValuesWithDefault).thenReturn(Future(staticMetadataRows))
  }
}
