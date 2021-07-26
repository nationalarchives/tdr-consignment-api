package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ClientFileMetadataFields.ClientFileMetadata
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}
import uk.gov.nationalarchives.Tables.FilemetadataRow
import uk.gov.nationalarchives.tdr.api.db.DbConnection

import scala.concurrent.{ExecutionContext, Future}

class ClientFileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getClientFileMetadata" should "return client file metadata given an existing file id" in {
    val fixedUuidSource = new FixedUUIDSource()
    val clientMetadataUuid = fixedUuidSource.uuid
    val fileUuid = UUID.randomUUID()
    val dummyInstant = Instant.now()
    val dummyTimestamp = Timestamp.from(dummyInstant)
    val dummyFileSize: Long = 1000
    val repositoryMock = mock[FileMetadataRepository]
    def row(name: String, value: String): FilemetadataRow =
      FilemetadataRow(clientMetadataUuid, fileUuid, value, Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), name)

    val mockResponse = Future(Seq(
      row(ClientSideFileSize, dummyFileSize.toString),
      row(ClientSideFileLastModifiedDate, dummyTimestamp.toString),
      row(ClientSideOriginalFilepath, "dummy/original/path"),
      row(SHA256ClientSideChecksum, "dummyCheckSum")
    ))

    when(repositoryMock.getFileMetadata(fileUuid, clientSideProperties:_*)).thenReturn(mockResponse)

    val service = new ClientFileMetadataService(repositoryMock)
    val result = service.getClientFileMetadata(fileUuid).futureValue

    result.fileId shouldBe fileUuid
    result.originalPath.get shouldBe "dummy/original/path"
    result.checksum.get shouldBe "dummyCheckSum"
    result.checksumType.get shouldBe "SHA256"
    result.lastModified shouldBe dummyInstant.toEpochMilli
    result.fileSize.get shouldBe dummyFileSize
  }

  "getClientFileMetadata" should "return an empty list for a non existent id" in {
    val fixedUuidSource = new FixedUUIDSource()
    val fileUuid = UUID.randomUUID()
    val repositoryMock = mock[FileMetadataRepository]
    when(repositoryMock.getFileMetadata(fileUuid, clientSideProperties:_*)).thenReturn(Future(Seq()))
    val service = new ClientFileMetadataService(repositoryMock)
    val caught: Throwable = service.getClientFileMetadata(fileUuid).failed.futureValue
    caught.getMessage should equal(s"Could not find client metadata for file $fileUuid")
  }
}
