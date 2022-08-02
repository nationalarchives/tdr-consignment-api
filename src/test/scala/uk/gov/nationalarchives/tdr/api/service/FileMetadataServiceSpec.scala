package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.util.UUID
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Mismatch, Success}

import scala.concurrent.{ExecutionContext, Future}

class FileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addFileMetadata" should "call the metadata repository with the correct row arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value",
        Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val fixedUUIDSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUUIDSource.uuid
    fixedUUIDSource.reset

    val addChecksumCaptor: ArgumentCaptor[FilemetadataRow] = ArgumentCaptor.forClass(classOf[FilemetadataRow])
    val getFileMetadataFileCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val getFileMetadataPropertyNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val mockFileMetadataResponse =
      Seq(FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ClientSideChecksum))

    when(metadataRepositoryMock.addChecksumMetadata(addChecksumCaptor.capture(), any[FilestatusRow]))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(getFileMetadataFileCaptor.capture(), getFileMetadataPropertyNameCaptor.capture()))
      .thenReturn(Future(mockFileMetadataResponse))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fixedFileUuid, "value"), fixedUserId).futureValue


    val row = addChecksumCaptor.getValue
    row.propertyname should equal(SHA256ServerSideChecksum)
    row.fileid should equal(fixedFileUuid)
    row.userid should equal(fixedUserId)
    row.datetime should equal(Timestamp.from(FixedTimeSource.now))
    row.metadataid.shouldBe(metadataId)
  }

  "addFileMetadata" should "call throw an error if the client side checksum is missing" in {
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val fileId = UUID.randomUUID()

    when(metadataRepositoryMock.getFileMetadataByProperty(fileId, SHA256ClientSideChecksum))
      .thenReturn(Future(Seq()))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val err =
      service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fileId, "value"), UUID.randomUUID()).failed.futureValue

    err.getMessage should equal(s"Could not find metadata for file $fileId")
  }

  "addFileMetadata" should "call the metadata repository with the correct arguments if the checksum matches" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch",
      timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value",
        Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val fileStatusCaptor: ArgumentCaptor[FilestatusRow] = ArgumentCaptor.forClass(classOf[FilestatusRow])
    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)

    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fixedFileUuid, "ChecksumMatch"), fixedUserId).futureValue
    fileStatusCaptor.getValue.fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue.statustype should equal("ChecksumMatch")
    fileStatusCaptor.getValue.value should equal(Success)
  }

  "addFileMetadata" should "call the metadata repository with the correct arguments if the checksum doesn't match" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value",
        Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch",
      timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val fileStatusCaptor: ArgumentCaptor[FilestatusRow] = ArgumentCaptor.forClass(classOf[FilestatusRow])
    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fixedFileUuid, "anotherchecksum"), fixedUserId).futureValue
    fileStatusCaptor.getValue.fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue.statustype should equal("ChecksumMatch")
    fileStatusCaptor.getValue.value should equal(Mismatch)
  }

  "addFileMetadata" should "return the correct data for a single update" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val value = "value"
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value",
        Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val propertyName = SHA256ServerSideChecksum
    val fixedUUIDSource = new FixedUUIDSource()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch",
      timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))


    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], any[FilestatusRow])).thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUUIDSource)
    val result: FileMetadataWithFileId =
      service.addFileMetadata(AddFileMetadataWithFileIdInput(propertyName, fixedFileUuid, "value"), fixedUserId).futureValue

    result.fileId should equal(fixedFileUuid)
    result.filePropertyName should equal(propertyName)
    result.value should equal(value)
  }

  "addFileMetadata" should "fail if the update is not for a checksum" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val fileId = UUID.randomUUID()

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val err = service.addFileMetadata(AddFileMetadataWithFileIdInput("SomethingElse", fileId, "ChecksumMatch"), UUID.randomUUID()).failed.futureValue
    err.getMessage should equal("SomethingElse found. We are only expecting checksum updates for now")
  }

  "addBulkFileMetadata" should "call the file and file metadata repository with the correct arguments" in {
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val fixedFolderFileUuid = UUID.fromString("f89da9b9-4c3b-4a17-a903-61c36b822c17")
    val filesInFolderFixedFileUuids = Seq(UUID.fromString("1a10e744-7292-426b-a36a-a6648152fa35"))
    val fileUuids: Seq[UUID] = fixedFolderFileUuid +: (0 until 49).map(_ => UUID.randomUUID())
    val propertyName1 = "propertyName1"
    val value1 = "value1"
    val timestamp = Timestamp.from(FixedTimeSource.now)

    val metadataProperties = Seq(
      AddFileMetadataInput(propertyName1, value1),
      AddFileMetadataInput("propertyName2", "value2"),
      AddFileMetadataInput("propertyName3", "value3")
    )

    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]

    val fixedUUIDSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUUIDSource.uuid
    fixedUUIDSource.reset

    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val addFileIdCaptor: ArgumentCaptor[Seq[UUID]] = ArgumentCaptor.forClass(classOf[Seq[UUID]])

    val mockFileResponse: Seq[FileRow] = generateFileRows(fileUuids, filesInFolderFixedFileUuids, fixedUserId, timestamp)
    val mockFileMetadataResponse: Seq[FilemetadataRow] = generateFileMetadataRows(fixedUserId, fileUuids, filesInFolderFixedFileUuids, metadataProperties)

    when(fileRepositoryMock.getAllDescendants(addFileIdCaptor.capture()))
      .thenReturn(Future(mockFileResponse))
    when(metadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture()))
      .thenReturn(Future(mockFileMetadataResponse))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addBulkFileMetadata(AddBulkFileMetadataInput(fileUuids, metadataProperties), fixedUserId).futureValue

    val addFileIdArgument = addFileIdCaptor.getValue
    val firstFileIdRow = addFileIdArgument.head

    firstFileIdRow should equal(fixedFolderFileUuid)
    addFileIdArgument.length + filesInFolderFixedFileUuids.length should equal(mockFileResponse.length)  // fileIds of files in folder should also be returned

    val addFileMetadataArgument = addFileMetadataCaptor.getValue
    val firstMetadataRow = addFileMetadataArgument.head

    addFileMetadataArgument.length should equal(mockFileMetadataResponse.length)
    firstMetadataRow.fileid should equal(filesInFolderFixedFileUuids.head) // the id should not be the folder's (fixedFolderFileUuid) but the file in the folder
    firstMetadataRow.propertyname should equal(propertyName1)
    firstMetadataRow.value should equal(value1)
    firstMetadataRow.userid should equal(fixedUserId)
    firstMetadataRow.datetime should equal(timestamp)
    firstMetadataRow.metadataid.shouldBe(metadataId)
  }

  "getFileMetadata" should "call the repository with the correct arguments" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()
    val mockResponse = Future(Seq())

    when(fileMetadataRepositoryMock.getFileMetadata(
      consignmentIdCaptor.capture(), selectedFileIdsCaptor.capture(), any[Option[Set[String]]]
    )).thenReturn(mockResponse)

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    service.getFileMetadata(consignmentId).futureValue
    consignmentIdCaptor.getValue should equal(consignmentId)
    selectedFileIdsCaptor.getValue should equal(None)
  }

  "getFileMetadata" should "return multiple map entries for multiple files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val mockResponse = Future(Seq(
      FilemetadataRow(UUID.randomUUID(), fileIdOne, "1", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "ClientSideFileSize"),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, "valueTwo", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "FoiExemptionCode")
    ))

    when(fileMetadataRepositoryMock.getFileMetadata(any[UUID], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(mockResponse)

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.getFileMetadata(consignmentId).futureValue

    response.size should equal(2)
    response.contains(fileIdOne) should equal(true)
    response.contains(fileIdTwo) should equal(true)
    response(fileIdOne).clientSideFileSize.get should equal(1)
    response(fileIdTwo).foiExemptionCode.get should equal("valueTwo")
  }

  private def generateFileRows(fileUuids: Seq[UUID], filesInFolderFixedFileUuids: Seq[UUID], fixedUserId: UUID, timestamp: Timestamp): Seq[FileRow] = {
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()

    val folderFileRow = Seq(
      FileRow(
        fileUuids.head, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName")
      )
    )

    val fileRowsForFileInFolder: Seq[FileRow] = filesInFolderFixedFileUuids.map(fileUuid =>
      FileRow(
        fileUuid, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileName"), Some(parentId)
      )
    )

    val folderAndFileRows = folderFileRow ++ fileRowsForFileInFolder

    val fileMetadataRowsExceptFirst: Seq[FileRow] = fileUuids.drop(1).map(fileUuid =>
      FileRow(
        fileUuid, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileName")
      )
    )

    folderAndFileRows ++ fileMetadataRowsExceptFirst
  }

  private def generateFileMetadataRows(fixedUserId: UUID, fileUuids: Seq[UUID], filesInFolderFixedFileUuids: Seq[UUID],
                                       metadataProperties: Seq[AddFileMetadataInput]): Seq[FilemetadataRow] = {
    val timestamp = Timestamp.from(FixedTimeSource.now)
    // 1st fileId is a folder, so instead, generate the metadata for its files
    val fileMetadataRowsForFirstFileId: Seq[FilemetadataRow] = filesInFolderFixedFileUuids.flatMap{
      fileUuid =>
        metadataProperties.map(fileMetadata =>
          FilemetadataRow(UUID.randomUUID(), fileUuid, fileMetadata.value, timestamp, fixedUserId, fileMetadata.filePropertyName)
        )
    }

    val moreFileMetadataRows: Seq[FilemetadataRow] = fileUuids.drop(1).flatMap{
      fileUuid =>
        metadataProperties.map(metadataProperty =>
            FilemetadataRow(
              UUID.randomUUID(),
              fileUuid,
              metadataProperty.value,
              timestamp,
              fixedUserId,
              metadataProperty.filePropertyName
            )
        )
    }
    fileMetadataRowsForFirstFileId ++ moreFileMetadataRows
  }
}
