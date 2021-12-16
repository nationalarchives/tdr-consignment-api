package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{AvmetadataRow, ConsignmentRow, FfidmetadataRow, FfidmetadatamatchesRow, FileRow, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, Files, ClientSideMetadataInput}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{File, FileMetadataValues, clientSideProperties, staticMetadataProperties}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  private val bodyId = UUID.randomUUID()

  val consignmentRepositoryMock: ConsignmentRepository = mock[ConsignmentRepository]
  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val antivirusMetadataRepositoryMock: AntivirusMetadataRepository = mock[AntivirusMetadataRepository]
  val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, new FixedUUIDSource())

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

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      mock[FileRepository], FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)

    val fileService = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, consignmentStatusRepositoryMock, fileMetadataService,
          ffidMetadataService, antivirusMetadataService, FixedTimeSource, fixedUuidSource
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

  "getFileMetadata" should "return the correct metadata" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
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
    val mockAvMetadataResponse = Future(
      Seq(AvmetadataRow(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp))
    )
    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId)).thenReturn(Future(fileMetadataRows))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(mockAvMetadataResponse)

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      fileRepositoryMock, FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, consignmentStatusRepositoryMock, fileMetadataService,
          ffidMetadataService, antivirusMetadataService, FixedTimeSource, fixedUuidSource
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
        datetime.getTime)),
      Option(AntivirusMetadata(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp.getTime))
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "getFileMetadata" should "return empty fields if the metadata has an unexpected property name" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
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

    val fileMetadataRows = Seq(
      fileMetadataRow(fileId, "customPropertyNameOne", "customValueOne"),
      fileMetadataRow(fileId, "customPropertyNameTwo", "customValueTwo")
    )
    when(fileMetadataRepositoryMock.getFileMetadata(consignmentId)).thenReturn(Future(fileMetadataRows))

    val fileMetadataService = new FileMetadataService(fileMetadataRepositoryMock, FixedTimeSource, fixedUuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository],
      fileRepositoryMock, FixedTimeSource, fixedUuidSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)

    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, consignmentStatusRepositoryMock, fileMetadataService,
          ffidMetadataService, antivirusMetadataService, FixedTimeSource, fixedUuidSource)

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
        datetime.getTime)),
      Option.empty
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "addFile" should "add a file and the client and static metadata" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]

    val fixedUuidSource = new FixedUUIDSource()
    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/nested/path/OriginalPath1", "Checksum1", 1L, 1L, 1)
    val metadataInputTwo = ClientSideMetadataInput("OriginalPath2", "Checksum2", 1L, 1L, 2)

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    val service = new FileService(
      fileRepositoryMock, consignmentRepositoryMock, consignmentStatusRepositoryMock, fileMetadataService,
            ffidMetadataService, antivirusMetadataService, FixedTimeSource, fixedUuidSource)

    val response = service.addFile(AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo)), userId).futureValue
    val fileRows: List[FileRow] = fileRowCaptor.getValue
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getValue

    response.head.fileId should equal(UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f"))
    response.head.matchId should equal(1)

    response.last.fileId should equal(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.last.matchId should equal(2)

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 18
    metadataRows.size should equal(expectedSize)
    staticMetadataProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop.name && r.value == prop.value) should equal(2)
    })

    clientSideProperties.foreach(prop => {
      metadataRows.count(r => r.propertyname == prop) should equal(2)
    })

    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  private def ffidMetadataRow(ffidMetadataid: UUID, fileId: UUID, datetime: Timestamp): FfidmetadataRow =
    FfidmetadataRow(ffidMetadataid, fileId, "pronom", "1.0", datetime, "signaturefileversion", "signature", "pronom")

  private def ffidMetadataMatchesRow(ffidMetadataid: UUID): FfidmetadatamatchesRow =
    FfidmetadatamatchesRow(ffidMetadataid, Some("txt"), "identification", Some("x-fmt/111"))

  private def fileMetadataRow(fileId: UUID, propertyName: String, value: String): FilemetadataRow =
    FilemetadataRow(UUID.randomUUID(), fileId, value, Timestamp.from(Instant.now()), UUID.randomUUID(), propertyName)
}
