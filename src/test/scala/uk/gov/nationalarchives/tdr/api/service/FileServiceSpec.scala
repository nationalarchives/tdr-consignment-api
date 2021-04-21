package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{ConsignmentRow, ConsignmentstatusRow, FfidmetadataRow, FfidmetadatamatchesRow, FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFilesInput, Files}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{File, FileMetadataValues, staticMetadataProperties}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentRepositoryMock: ConsignmentRepository = mock[ConsignmentRepository]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]

  "createFile" should "create a file given correct arguments" in {
    val fixedUuidSource = new FixedUUIDSource()
    val uuid = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()

    val mockFileResponse = Future.successful(List(FileRow(fileId, consignmentId, uuid, Timestamp.from(Instant.now))))
    when(fileRepositoryMock.addFiles(any[List[FileRow]], any[ConsignmentstatusRow])).thenReturn(mockFileResponse)
    val mockConsignmentResponse = Future.successful(())
    when(consignmentRepositoryMock.addParentFolder(consignmentId, "Parent folder name")).thenReturn(mockConsignmentResponse)
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileId, "value", Timestamp.from(Instant.now), uuid, "name")))
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[FilemetadataRow]])).thenReturn(mockFileMetadataResponse)
    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)
    val fileService = new FileService(fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource)
    val result: Files = fileService.addFile(AddFilesInput(consignmentId, 1, "Parent folder name"), uuid).futureValue

    result.fileIds shouldBe List(fileId)
  }

  "createFile" should "create three files given correct arguments" in {
    val fixedUuidSource = new FixedUUIDSource()
    val fileUuidOne = fixedUuidSource.uuid
    val fileUuidTwo = fixedUuidSource.uuid
    val fileUuidThree = fixedUuidSource.uuid
    fixedUuidSource.reset
    val consignmentUuid = UUID.randomUUID()
    val userUuid = UUID.randomUUID()

    val fileRowOne = FileRow(fileUuidOne, consignmentUuid, userUuid, Timestamp.from(FixedTimeSource.now))
    val fileRowTwo = FileRow(fileUuidTwo, consignmentUuid, userUuid, Timestamp.from(FixedTimeSource.now))
    val fileRowThree = FileRow(fileUuidThree, consignmentUuid, userUuid, Timestamp.from(FixedTimeSource.now))
    val captor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])

    val mockResponse = Future.successful(List(fileRowOne, fileRowTwo, fileRowThree))
    when(fileRepositoryMock.addFiles(captor.capture(), any[ConsignmentstatusRow])).thenReturn(mockResponse)
    val mockConsignmentResponse = Future.successful(())
    when(consignmentRepositoryMock.addParentFolder(consignmentUuid, "Parent folder name")).thenReturn(mockConsignmentResponse)
    val mockFileMetadataResponse = Future.successful(
      Seq(FilemetadataRow(UUID.randomUUID(), fileUuidOne, "value", Timestamp.from(Instant.now), userUuid, "name"))
    )
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[FilemetadataRow]])).thenReturn(mockFileMetadataResponse)
    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val fileService = new FileService(fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource)
    val result: Files = fileService.addFile(AddFilesInput(consignmentUuid, 3, "Parent folder name"), userUuid).futureValue

    captor.getAllValues.size should equal(1)
    captor.getAllValues.get(0).length should equal(3)
    captor.getAllValues.get(0).forall(List(fileRowOne, fileRowTwo, fileRowThree).contains) should be(true)

    result.fileIds shouldBe List(fileUuidOne, fileUuidTwo, fileUuidThree)
  }

  "createFile" should "link a file to the correct user, consignment, and consignment status" in {
    val fixedUuidSource = new FixedUUIDSource()
    val userId = UUID.randomUUID()
    val fileUuid = fixedUuidSource.uuid
    val consignmentUuid = UUID.randomUUID()
    val consignmentStatusUuid = fixedUuidSource.uuid
    val fileRepositoryMock = mock[FileRepository]
    val consignmentRepositoryMock = mock[ConsignmentRepository]
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val fileService = new FileService(fileRepositoryMock, consignmentRepositoryMock,fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource)
    fixedUuidSource.reset
    val timeNow = Timestamp.from(FixedTimeSource.now)

    val mockConsignmentStatusRow = ConsignmentstatusRow(consignmentStatusUuid, consignmentUuid, "Upload", "InProgress", timeNow)
    val expectedFileRow = List(FileRow(fileUuid, consignmentUuid, userId, timeNow))
    val fileCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val consignmentStatusCaptor: ArgumentCaptor[ConsignmentstatusRow] = ArgumentCaptor.forClass(classOf[ConsignmentstatusRow])
    val mockResponse = Future.successful(List(FileRow(fileUuid, consignmentUuid, userId, timeNow)))
    when(fileRepositoryMock.addFiles(fileCaptor.capture(), consignmentStatusCaptor.capture())).thenReturn(mockResponse)
    val mockConsignmentResponse = Future.successful(())
    when(consignmentRepositoryMock.addParentFolder(consignmentUuid, "Parent folder name")).thenReturn(mockConsignmentResponse)
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileUuid, "value", timeNow, userId, "name")))
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[FilemetadataRow]])).thenReturn(mockFileMetadataResponse)

    fileService.addFile(AddFilesInput(consignmentUuid, 1, "Parent folder name"), userId).futureValue

    verify(fileRepositoryMock).addFiles(expectedFileRow, mockConsignmentStatusRow)
    fileCaptor.getAllValues.size should equal(1)
    fileCaptor.getAllValues.get(0).head.consignmentid should equal(consignmentUuid)
    fileCaptor.getAllValues.get(0).head.userid should equal(userId)
    consignmentStatusCaptor.getAllValues.size() should equal(1)
    consignmentStatusCaptor.getAllValues.get(0).consignmentstatusid should equal(consignmentStatusUuid)
    consignmentStatusCaptor.getAllValues.get(0).consignmentid should equal(consignmentUuid)
    consignmentStatusCaptor.getAllValues.get(0).statustype should equal("Upload")
    consignmentStatusCaptor.getAllValues.get(0).value should equal("InProgress")
    consignmentStatusCaptor.getAllValues.get(0).createddatetime should equal(timeNow)
  }

  "createFile" should "create the correct static metadata" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val uuid = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()

    val mockFileResponse = Future.successful(List(FileRow(fileId, consignmentId, uuid, Timestamp.from(Instant.now))))
    when(fileRepositoryMock.addFiles(any[List[FileRow]], any[ConsignmentstatusRow])).thenReturn(mockFileResponse)
    val mockConsignmentResponse = Future.successful(())
    when(consignmentRepositoryMock.addParentFolder(consignmentId, "Parent folder name")).thenReturn(mockConsignmentResponse)
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileId, "value", Timestamp.from(Instant.now), uuid, "name")))

    val captor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    when(fileMetadataRepositoryMock.addFileMetadata(captor.capture())).thenReturn(mockFileMetadataResponse)

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUUIDSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUUIDSource)

    val fileService = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUUIDSource
    )
    fileService.addFile(AddFilesInput(consignmentId, 1, "Parent folder name"), uuid).futureValue

    val metadataRows: Seq[Tables.FilemetadataRow] = captor.getValue
    metadataRows.size should equal(staticMetadataProperties.size)

    def checkRow(propertyName: String, propertyValue: String): Assertion = {
      val filteredRow = metadataRows.find(_.propertyname == propertyName)
      filteredRow.isDefined should equal(true)
      filteredRow.get.value should equal(propertyValue)
      filteredRow.get.fileid should equal(fileId)
    }

    checkRow("RightsCopyright", "Crown Copyright")
    checkRow("LegalStatus", "Public Record")
    checkRow("HeldBy", "TNA")
    checkRow("Language", "English")
    checkRow("FoiExemptionCode", "open")
  }

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
      seriesId1,
      userId1,
      Timestamp.from(Instant.now),
      consignmentsequence = Option(400L),
      consignmentreference = "TEST-TDR-2021-VB"
    )
    val consignment2 = ConsignmentRow(
      consignmentId2,
      seriesId2,
      userId2,
      Timestamp.from(Instant.now),
      consignmentsequence = Option(500L),
      consignmentreference = "TEST-TDR-2021-3B"
    )

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val fileService = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource
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

  "getFiles" should "return all the files" in {
    val fixedUuidSource = new FixedUUIDSource()
    val uuid = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()

    val mockFileResponse = Future.successful(List(
      FileRow(fileIdOne, consignmentId, uuid, Timestamp.from(Instant.now)),
      FileRow(fileIdTwo, consignmentId, uuid, Timestamp.from(Instant.now))
    ))
    when(fileRepositoryMock.getFilesWithPassedAntivirus(any[UUID])).thenReturn(mockFileResponse)
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileIdOne, "value", Timestamp.from(Instant.now), uuid, "name")))
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[FilemetadataRow]])).thenReturn(mockFileMetadataResponse)

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val fileService = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource
    )
    val result: Files = fileService.getFiles(consignmentId).futureValue

    result.fileIds shouldBe List(fileIdOne, fileIdTwo)
  }

  "getFileMetadata" should "return the correct metadata" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))

    val fileMetadataRows = Seq(
      fileMetadataRow(fileId, "ClientSideFileLastModifiedDate", timestamp.toString),
      fileMetadataRow(fileId, "SHA256ClientSideChecksum", "checksum"),
      fileMetadataRow(fileId, "ClientSideOriginalFilepath", "filePath"),
      fileMetadataRow(fileId, "ClientSideFileSize", "1"),
      fileMetadataRow(fileId, "RightsCopyright", "rightsCopyright"),
      fileMetadataRow(fileId, "LegalStatus", "legalStatus"),
      fileMetadataRow(fileId, "HeldBy", "heldBy"),
      fileMetadataRow(fileId, "Language", "language"),
      fileMetadataRow(fileId, "FoiExemptionCode", "foiExemption")
    )
    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId)).thenReturn(Future(fileMetadataRows))

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource
    )
    val metadataList: Seq[File] = service.getFileMetadata(consignmentId).futureValue

    metadataList.length should equal(1)

    val actualFileMetadata: File = metadataList.head
    val expectedFileMetadata = File(fileId,
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
      Some(FFIDMetadata(
        fileId,
        "pronom",
        "1.0",
        "signaturefileversion",
        "signature",
        "pronom",
        List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"))),
        datetime.getTime))
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "getFileMetadata" should "return empty fields if the metadata has an unexpected property name" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))

    val fileMetadataRows = Seq(
      fileMetadataRow(fileId, "customPropertyNameOne", "customValueOne"),
      fileMetadataRow(fileId, "customPropertyNameTwo", "customValueTwo")
    )
    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId)).thenReturn(Future(fileMetadataRows))

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, fixedUuidSource)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, fileMetadataService, ffidMetadataService, FixedTimeSource, fixedUuidSource)
    val fileMetadataList = service.getFileMetadata(consignmentId).futureValue

    fileMetadataList.length should equal(1)

    val actualFileMetadata = fileMetadataList.head
    val expectedFileMetadata = File(fileId,
      FileMetadataValues(None, None, None, None, None, None, None, None, None),
      Some(FFIDMetadata(
        fileId,
        "pronom",
        "1.0",
        "signaturefileversion",
        "signature",
        "pronom",
        List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"))),
        datetime.getTime))
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  private def ffidMetadataRow(ffidMetadataid: UUID, fileId: UUID, datetime: Timestamp): FfidmetadataRow =
    FfidmetadataRow(ffidMetadataid, fileId, "pronom", "1.0", datetime, "signaturefileversion", "signature", "pronom")

  private def ffidMetadataMatchesRow(ffidMetadataid: UUID): FfidmetadatamatchesRow =
    FfidmetadatamatchesRow(ffidMetadataid, Some("txt"), "identification", Some("x-fmt/111"))

  private def fileMetadataRow(fileId: UUID, propertyName: String, value: String): FilemetadataRow =
    FilemetadataRow(UUID.randomUUID(), fileId, value, Timestamp.from(Instant.now()), UUID.randomUUID(), propertyName)
}
