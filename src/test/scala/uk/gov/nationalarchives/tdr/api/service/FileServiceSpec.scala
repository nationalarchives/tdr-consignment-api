package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sangria.relay.DefaultConnection
import uk.gov.nationalarchives.Tables.{AvmetadataRow, ConsignmentRow, FfidmetadataRow, FfidmetadatamatchesRow, FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.PaginationInput
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.FFID
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class FileServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  private val bodyId = UUID.randomUUID()

  val consignmentRepositoryMock: ConsignmentRepository = mock[ConsignmentRepository]
  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val antivirusMetadataRepositoryMock: AntivirusMetadataRepository = mock[AntivirusMetadataRepository]
  val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, new FixedUUIDSource())
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]

  //scalastyle:off magic.number
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

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      mock[FileRepository], mock[AllowedPuidsRepository], mock[DisallowedPuidsRepository], FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val fileService = new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
          ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load()
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
  //scalastyle:on magic.number

  "getFileMetadata" should "return all the correct files and folders with the correct metadata from the database response" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val allowedPuidsRepositoryMock = mock[AllowedPuidsRepository]
    val disallowedPuidsRepositoryMock = mock[DisallowedPuidsRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val timestamp = Timestamp.from(FixedTimeSource.now)
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val fileIdThree = UUID.randomUUID()

    val parentFolderRow = FileRow(
      parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))
    val fileOneRow = FileRow(
      fileIdOne, consignmentId, userId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileOneName"), Some(parentFolderId))
    val fileTwoRow = FileRow(
      fileIdTwo, consignmentId, userId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileTwoName"), Some(parentFolderId))
    val fileThreeRow = FileRow(
      fileIdThree, consignmentId, userId, timestamp, Some(true),
      Some(NodeType.fileTypeIdentifier), Some("fileThreeName"), Some(parentFolderId))

    val fileAndMetadataRows: Seq[(FileRow, Option[FilemetadataRow])] = Seq(
      (fileOneRow, Some(fileMetadataRow(fileIdOne, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileOneRow, Some(fileMetadataRow(fileIdOne, "SHA256ClientSideChecksum", "checksum"))),
      (fileTwoRow, Some(fileMetadataRow(fileIdTwo, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileTwoRow, Some(fileMetadataRow(fileIdTwo, "SHA256ClientSideChecksum", "checksum"))),
      (fileThreeRow, Some(fileMetadataRow(fileIdThree, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (parentFolderRow, None)
    )
    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), "FFID","Success", timestamp))
    )

    when(fileRepositoryMock.getFiles(consignmentId, FileFilters(None)))
      .thenReturn(Future(fileAndMetadataRows))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(List()))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, "FFID")).thenReturn(mockFileStatusResponse)

    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      fileRepositoryMock, allowedPuidsRepositoryMock, disallowedPuidsRepositoryMock, FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, ffidMetadataService, antivirusMetadataService,
      fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load()
    )

    val files = service.getFileMetadata(consignmentId).futureValue
    files.size shouldBe 4

    val parentFolder = files.find(_.fileId == parentFolderId).get
    parentFolder.fileName.get shouldBe "folderName"
    parentFolder.metadata shouldBe FileMetadataValues(None, None, None, None, None, None, None, None, None)

    val fileOne = files.find(_.fileId == fileIdOne).get
    fileOne.fileName.get shouldBe "fileOneName"
    fileOne.metadata shouldBe FileMetadataValues(Some("checksum"),None,Some(timestamp.toLocalDateTime),None,None,None,None,None,None)

    val fileTwo = files.find(_.fileId == fileIdTwo).get
    fileTwo.fileName.get shouldBe "fileTwoName"
    fileTwo.metadata shouldBe FileMetadataValues(Some("checksum"),None,Some(timestamp.toLocalDateTime),None,None,None,None,None,None)

    val fileThree = files.find(_.fileId == fileIdThree).get
    fileThree.fileName.get shouldBe "fileThreeName"
    fileThree.metadata shouldBe FileMetadataValues(None,None,Some(timestamp.toLocalDateTime),None,None,None,None,None,None)
  }

  "getFileMetadata" should "return the correct metadata" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
    val allowedPuidsRepositoryMock = mock[AllowedPuidsRepository]
    val disallowedPuidsRepositoryMock = mock[DisallowedPuidsRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))

    val fileRow = FileRow(
      fileId, consignmentId, userId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileName"), Some(parentId))

    val fileAndMetadataRows = Seq(
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileRow, Some(fileMetadataRow(fileId, "SHA256ClientSideChecksum", "checksum"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideOriginalFilepath", "filePath"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideFileSize", "1"))),
      (fileRow, Some(fileMetadataRow(fileId, "RightsCopyright", "rightsCopyright"))),
      (fileRow, Some(fileMetadataRow(fileId, "LegalStatus", "legalStatus"))),
      (fileRow, Some(fileMetadataRow(fileId, "HeldBy", "heldBy"))),
      (fileRow, Some(fileMetadataRow(fileId, "Language", "language"))),
      (fileRow, Some(fileMetadataRow(fileId, "FoiExemptionCode", "foiExemption")))
    )

    val mockAvMetadataResponse = Future(
      Seq(AvmetadataRow(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp))
    )

    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(),fileId, "FFID","Success",timestamp))
    )

    when(fileRepositoryMock.getFiles(consignmentId, FileFilters()))
      .thenReturn(Future(fileAndMetadataRows))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(mockAvMetadataResponse)
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, "FFID")).thenReturn(mockFileStatusResponse)

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      fileRepositoryMock, allowedPuidsRepositoryMock, disallowedPuidsRepositoryMock, FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
          ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load()
    )

    val fileList: Seq[File] = service.getFileMetadata(consignmentId).futureValue

    fileList.length should equal(1)

    val actualFileMetadata: File = fileList.head
    val expectedFileMetadata = File(fileId,
      Some(NodeType.fileTypeIdentifier),
      Some("fileName"),
      Some(parentId),
      FileMetadataValues(
        Some("checksum"),
        Some("filePath"),
        Some(timestamp.toLocalDateTime),
        Some(1),
        Some("rightsCopyright"),
        Some("legalStatus"),
        Some("heldBy"),
        Some("language"),
        Some("foiExemption")),
      Some("Success"),
      Some(FFIDMetadata(
        fileId,
        "pronom",
        "1.0",
        "signaturefileversion",
        "signature",
        "pronom",
        List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"))),
        datetime.getTime)),
      Option(AntivirusMetadata(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp.getTime))
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "getFileMetadata" should "return empty fields if the metadata has an unexpected property name and no file data" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
    val allowedPuidsRepositoryMock = mock[AllowedPuidsRepository]
    val disallowedPuidsRepositoryMock = mock[DisallowedPuidsRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))

    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(),fileId, "FFID","Success",datetime))
    )

    val fileRow = FileRow(fileId, consignmentId, userId, Timestamp.from(Instant.now))
    val fileAndMetadataRows = Seq(
      (fileRow, Some(fileMetadataRow(fileId, "customPropertyNameOne", "customValueOne"))),
      (fileRow, Some(fileMetadataRow(fileId, "customPropertyNameTwo", "customValueTwo")))
    )
    when(fileRepositoryMock.getFiles(consignmentId, FileFilters(None))).thenReturn(Future(fileAndMetadataRows))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, "FFID")).thenReturn(mockFileStatusResponse)

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      fileRepositoryMock, allowedPuidsRepositoryMock, disallowedPuidsRepositoryMock, FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
          ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load())

    val fileMetadataList = service.getFileMetadata(consignmentId).futureValue

    fileMetadataList.length should equal(1)

    val actualFileMetadata = fileMetadataList.head
    val expectedFileMetadata = File(fileId, None, None, None,
      FileMetadataValues(None, None, None, None, None, None, None, None, None),
      Some("Success"),
      Some(FFIDMetadata(
        fileId,
        "pronom",
        "1.0",
        "signaturefileversion",
        "signature",
        "pronom",
        List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"))),
        datetime.getTime)),
      Option.empty
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are greater than batch size" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val fixedUuidSource = new FixedUUIDSource()
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/nested/path/OriginalPath1", "Checksum1", 1L, 1L, 1)
    val metadataInputTwo = ClientSideMetadataInput("OriginalPath2", "Checksum2", 1L, 1L, 2)

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
            ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load())

    val response = service.addFile(AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo)), userId).futureValue

    verify(fileRepositoryMock, times(2)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList


    response.head.fileId should equal(UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f"))
    response.head.matchId should equal(2)

    response.last.fileId should equal(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.last.matchId should equal(1)

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 36
    metadataRows.size should equal(expectedSize)
    staticMetadataProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop.name && r.value == prop.value) should equal(5)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath => count should equal(5) //Directories have this set
        case _ => count should equal(2)
      }

    })

    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are less than batch size" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val fixedUuidSource = new FixedUUIDSource()
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/OriginalPath1", "Checksum1", 1L, 1L, 1)
    val metadataInputTwo = ClientSideMetadataInput("OriginalPath2", "Checksum2", 1L, 1L, 2)

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
            ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load())

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
    val expectedSize = 24
    metadataRows.size should equal(expectedSize)
    staticMetadataProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop.name && r.value == prop.value) should equal(3)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath => count should equal(3) //Directories have this set
        case _ => count should equal(2)
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
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1.toString), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, FFID, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesOrFoldersInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, page, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input).futureValue

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
    val input = Some(PaginationInput(Some(limitExceedingMax), Some(currentPage), Some(fileId1.toString), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, FFID, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesOrFoldersInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))


    when(fileRepositoryMock.getPaginatedFiles(consignmentId, expectedMaxLimit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input).futureValue

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
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, FFID, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesOrFoldersInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, None, FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe false

    edges.size shouldBe 2
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
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1.toString), Some(fileFilters)))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, FFID, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesOrFoldersInConsignment(consignmentId, fileFilters.selectedFilesId, fileFilters.fileTypeIdentifier))
      .thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), fileFilters))
      .thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input).futureValue
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
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, FFID, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesOrFoldersInConsignment(consignmentId, None, None)).thenReturn(Future.successful(0))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1.toString), None))

    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor shouldBe None
    pageInfo.endCursor shouldBe None
    pageInfo.hasNextPage shouldBe false
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 0
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
    val thrownException = intercept[Exception] { fileService.getPaginatedFiles(consignmentId, None).futureValue }

    thrownException.getMessage should equal("No pagination input argument provided for 'paginatedFiles' field query")
  }

  "getConsignmentParentFolderId" should "return the parent folder id for a given consignment" in {
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val parentFolderRow = FileRow(
      parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))

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

  private def setupFileService(fileRepositoryMock: FileRepository): FileService = {
    val fixedUuidSource = new FixedUUIDSource()
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      mock[FileRepository], mock[AllowedPuidsRepository], mock[DisallowedPuidsRepository], FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    new FileService(
      fileRepositoryMock, consignmentRepositoryMock,
      ffidMetadataService, antivirusMetadataService, fileStatusService, fileMetadataService, FixedTimeSource, fixedUuidSource, ConfigFactory.load())
  }

  private def ffidMetadataRow(ffidMetadataid: UUID, fileId: UUID, datetime: Timestamp): FfidmetadataRow =
    FfidmetadataRow(ffidMetadataid, fileId, "pronom", "1.0", datetime, "signaturefileversion", "signature", "pronom")

  private def ffidMetadataMatchesRow(ffidMetadataid: UUID): FfidmetadatamatchesRow =
    FfidmetadatamatchesRow(ffidMetadataid, Some("txt"), "identification", Some("x-fmt/111"))

  private def fileMetadataRow(fileId: UUID, propertyName: String, value: String): FilemetadataRow =
    FilemetadataRow(UUID.randomUUID(), fileId, value, Timestamp.from(Instant.now()), UUID.randomUUID(), propertyName)

  private def createFileRow(id: UUID, consignmentId: UUID, fileName: String, parentId: UUID): FileRow = {
    FileRow(
      id, consignmentId, userId, Timestamp.from(FixedTimeSource.now), Some(true), Some(NodeType.fileTypeIdentifier), Some(fileName), Some(parentId))
  }
}
