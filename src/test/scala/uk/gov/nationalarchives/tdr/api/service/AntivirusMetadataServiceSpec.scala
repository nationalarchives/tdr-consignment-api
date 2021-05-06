package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{AvmetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.AntivirusMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AddAntivirusMetadataInput
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addAntivirusMetadata" should "create anti-virus metadata given the correct arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val dummyInstant = Instant.now()
    val avRepositoryMock = mock[AntivirusMetadataRepository]
    val mockResponse = mockAvResponse(fixedFileUuid, "result")

    when(avRepositoryMock.addAntivirusMetadata(any[AvmetadataRow], any[FilestatusRow])).thenReturn(mockResponse)

    val service: AntivirusMetadataService = new AntivirusMetadataService(avRepositoryMock, new FixedUUIDSource(), FixedTimeSource)
    val result = service.addAntivirusMetadata(avServiceInput(fixedFileUuid, "result")).futureValue

    result.fileId shouldBe fixedFileUuid
    result.software shouldBe "software"
    result.softwareVersion shouldBe "software version"
    result.databaseVersion shouldBe "database version"
    result.result shouldBe "result"
    result.datetime shouldBe Timestamp.from(FixedTimeSource.now).getTime
  }

  "addAntivirusMetadata" should "update the file status table with success when a virus is not found" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val avRepositoryMock = mock[AntivirusMetadataRepository]
    val mockResponse = mockAvResponse(fixedFileUuid, "")

    val fileStatusCaptor: ArgumentCaptor[FilestatusRow] = ArgumentCaptor.forClass(classOf[FilestatusRow])

    when(avRepositoryMock.addAntivirusMetadata(any[AvmetadataRow], fileStatusCaptor.capture())).thenReturn(mockResponse)

    val service: AntivirusMetadataService = new AntivirusMetadataService(avRepositoryMock, new FixedUUIDSource(), FixedTimeSource)
    val result = ""
    service.addAntivirusMetadata(avServiceInput(fixedFileUuid, result)).futureValue

    val fileStatusRow = fileStatusCaptor.getValue
    fileStatusRow.statustype should equal(Antivirus)
    fileStatusRow.value should equal(Success)
  }

  "addAntivirusMetadata" should "update the file status table with virus found when a virus is found" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val dummyTimestamp = Timestamp.from(FixedTimeSource.now)
    val avRepositoryMock = mock[AntivirusMetadataRepository]
    val mockResponse = mockAvResponse(fixedFileUuid, "result")

    val fileStatusCaptor: ArgumentCaptor[FilestatusRow] = ArgumentCaptor.forClass(classOf[FilestatusRow])

    when(avRepositoryMock.addAntivirusMetadata(any[AvmetadataRow], fileStatusCaptor.capture())).thenReturn(mockResponse)

    val service: AntivirusMetadataService = new AntivirusMetadataService(avRepositoryMock, new FixedUUIDSource(), FixedTimeSource)
    service.addAntivirusMetadata(avServiceInput(fixedFileUuid, "result")).futureValue

    val fileStatusRow = fileStatusCaptor.getValue
    fileStatusRow.statustype should equal(Antivirus)
    fileStatusRow.value should equal(VirusDetected)
  }

  "getAntivirusMetadata" should "call the repository with the correct arguments" in {
    val avRepositoryMock = mock[AntivirusMetadataRepository]
    val consignmentCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val consignmentId = UUID.randomUUID()
    when(avRepositoryMock.getAntivirusMetadata(consignmentCaptor.capture())).thenReturn(Future(Seq()))
    new AntivirusMetadataService(avRepositoryMock, new FixedUUIDSource(), FixedTimeSource).getAntivirusMetadata(consignmentId).futureValue
    consignmentCaptor.getValue should equal(consignmentId)
  }

  "getAntivirusMetadata" should "return the correct data" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val dummyTimestamp = Timestamp.from(FixedTimeSource.now)
    val consignmentId = UUID.randomUUID()
    val avRepositoryMock = mock[AntivirusMetadataRepository]
    val mockResponse = Future.successful(Seq(AvmetadataRow(
      fixedFileUuid,
      "software",
      "software version",
      "database version",
      "result",
      dummyTimestamp
    )))

    when(avRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(mockResponse)
    val response = new AntivirusMetadataService(avRepositoryMock, new FixedUUIDSource(), FixedTimeSource).getAntivirusMetadata(consignmentId).futureValue
    val antivirus = response.head
    antivirus.fileId should equal(fixedFileUuid)
    antivirus.databaseVersion should equal("database version")
    antivirus.result should equal("result")
    antivirus.software should equal("software")
    antivirus.softwareVersion should equal("software version")
    antivirus.datetime should equal(dummyTimestamp.getTime)
  }

  private def mockAvResponse(fixedFileUuid: UUID, result: String): Future[AvmetadataRow] = {
    Future.successful(AvmetadataRow(
      fixedFileUuid,
      "software",
      "software version",
      "database version",
      result,
      Timestamp.from(FixedTimeSource.now)
    ))
  }

  private def avServiceInput(fixedFileUuid: UUID, result: String): AddAntivirusMetadataInput = {
    AddAntivirusMetadataInput(
      fixedFileUuid,
      "software",
      "software version",
      "database version",
      result,
      Timestamp.from(FixedTimeSource.now).getTime
    )
  }
}
