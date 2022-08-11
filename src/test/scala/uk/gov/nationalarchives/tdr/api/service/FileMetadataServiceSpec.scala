package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileMetadataUpdate, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Mismatch, Success}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.util.UUID
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

  "updateBulkFileMetadata" should "call the file repository with the correct arguments, get all of the file metadata for a consignment " +
    "and then add metadata rows that haven't already been added and update those that have" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val (mockGetFileMetadataResponse, mockAddFileMetadataResponse): (Seq[FilemetadataRow], Seq[FilemetadataRow]) =
      generateFileMetadataRows(
        testSetUp.fixedUserId,
        testSetUp.fileUuids,
        testSetUp.filesInFolderFixedFileUuids,
        testSetUp.metadataPropertiesWithOldValues,
        testSetUp.metadataPropertiesWithNewValues
      )

    when(testSetUp.fileRepositoryMock.getAllDescendants(testSetUp.getDescendentsFileIdsCaptor.capture()))
      .thenReturn(Future(testSetUp.mockFileResponse))
    when(testSetUp.metadataRepositoryMock.getFileMetadata(
      testSetUp.consignmentIdCaptor.capture(), testSetUp.getFileMetadataIds.capture(), testSetUp.getFileMetadataPropertyNames.capture()))
      .thenReturn(Future(mockGetFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.addFileMetadata(testSetUp.addFileMetadataCaptor.capture()))
      .thenReturn(Future(mockAddFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.updateFileMetadataProperties(testSetUp.updateFileMetadataPropsArgCaptor.capture()))
      .thenReturn(
        Future(
          Seq("propertyName1", "propertyName2", "propertyName3").map(propertyName => mockGetFileMetadataResponse.count(_.propertyname == propertyName))
       )
      )

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.fileUuids, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.fixedUserId
    ).futureValue

    val getFileIdsArgument: Seq[UUID] = testSetUp.getDescendentsFileIdsCaptor.getValue
    val firstFileIdRow: UUID = getFileIdsArgument.head

    firstFileIdRow should equal(testSetUp.fixedFolderFileUuid)

    val fileIdsPassedIntoFileMetadata = getFileIdsArgument.length + testSetUp.filesInFolderFixedFileUuids.length
    fileIdsPassedIntoFileMetadata should equal(testSetUp.mockFileResponse.length) // fileIds of files in folder should also be returned

    val consignmentIdArgument = testSetUp.consignmentIdCaptor.getValue
    val getFileMetadataIdsArgument = testSetUp.getFileMetadataIds.getValue
    val addFileMetadataArgument = testSetUp.addFileMetadataCaptor.getValue
    val fileIdsPassedInToAddFileMetadata = addFileMetadataArgument.map(_.fileid)
    val valuesPassedInToAddFileMetadata = addFileMetadataArgument.map(_.value)
    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue

    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap{
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    consignmentIdArgument should equal(testSetUp.consignmentId)
    getFileMetadataIdsArgument.get should equal((testSetUp.filesInFolderFixedFileUuids ++ testSetUp.fileUuids.drop(1)).toSet)
    addFileMetadataArgument.nonEmpty should equal(true)
    addFileMetadataArgument.map(_.fileid).sorted should equal(mockAddFileMetadataResponse.map(_.fileid).toVector.sorted)
    fileIdsPassedInToAddFileMetadata.contains(testSetUp.filesInFolderFixedFileUuids.head) should equal(true)
    // the folder's (fixedFolderFileUuid) id should not be added only the file(s) within it
    fileIdsPassedInToAddFileMetadata.contains(testSetUp.fixedFolderFileUuid) should equal(false)
    // file Ids that go into addFileMetadata should be the same as the stub for the addFileMetadata response
    fileIdsPassedInToAddFileMetadata.sorted === mockAddFileMetadataResponse.map(_.fileid).sorted should equal(true)
    valuesPassedInToAddFileMetadata.forall(_.startsWith("newValue")) should equal(true)
    // metadata Ids that go into updateFileMetadata should be the same as the ones that were taken from the getFileMetadata
    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(mockGetFileMetadataResponse.map(_.metadataid).sorted)
  }

  "updateBulkFileMetadata" should "pass into 'updateFileMetadata', only the metadataIds where the " +
    "'value' (of its FileMetadata row) differs from the value the user is trying to set" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val newMetadataPropertiesWithOneOldValue = Seq(
      UpdateFileMetadataInput("propertyName1", "value1"), // this value already exists on propertyName1, therefore, there is no need to update it
      UpdateFileMetadataInput("propertyName2", "newValue2"),
      UpdateFileMetadataInput("propertyName3", "newValue3")
    )

    val (mockGetFileMetadataResponse, mockAddFileMetadataResponse): (Seq[FilemetadataRow], Seq[FilemetadataRow]) =
      generateFileMetadataRows(
        testSetUp.fixedUserId,
        testSetUp.fileUuids,
        testSetUp.filesInFolderFixedFileUuids,
        testSetUp.metadataPropertiesWithOldValues,
        newMetadataPropertiesWithOneOldValue
      )

    when(testSetUp.fileRepositoryMock.getAllDescendants(testSetUp.getDescendentsFileIdsCaptor.capture()))
      .thenReturn(Future(testSetUp.mockFileResponse))
    when(testSetUp.metadataRepositoryMock.getFileMetadata(
      testSetUp.consignmentIdCaptor.capture(), testSetUp.getFileMetadataIds.capture(), testSetUp.getFileMetadataPropertyNames.capture()
    )).thenReturn(Future(mockGetFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.addFileMetadata(testSetUp.addFileMetadataCaptor.capture()))
      .thenReturn(Future(mockAddFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.updateFileMetadataProperties(testSetUp.updateFileMetadataPropsArgCaptor.capture()))
      .thenReturn( // leave out propertyName1 because it would not have been updated
        Future(Seq("propertyName2", "propertyName3").map(propertyName => mockGetFileMetadataResponse.count(_.propertyname == propertyName)))
    )

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.fileUuids, newMetadataPropertiesWithOneOldValue),
      testSetUp.fixedUserId
    ).futureValue

    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue

    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap{
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    val metadataIdsWithoutOldValue: Seq[UUID] = mockGetFileMetadataResponse.collect {
      case fileMetadataRow if fileMetadataRow.value != "value1" => fileMetadataRow.metadataid
    }

    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(metadataIdsWithoutOldValue.sorted)
  }

  "updateBulkFileMetadata" should "only add metadata rows but not update any" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val (mockGetFileMetadataResponse, mockAddFileMetadataResponse): (Seq[FilemetadataRow], Seq[FilemetadataRow]) =
      generateFileMetadataRows(
        testSetUp.fixedUserId,
        testSetUp.fileUuids,
        testSetUp.filesInFolderFixedFileUuids,
        testSetUp.metadataPropertiesWithOldValues,
        testSetUp.metadataPropertiesWithNewValues,
        Seq("add")
      )

    when(testSetUp.fileRepositoryMock.getAllDescendants(testSetUp.getDescendentsFileIdsCaptor.capture()))
      .thenReturn(Future(testSetUp.mockFileResponse))
    when(testSetUp.metadataRepositoryMock.getFileMetadata(
      testSetUp.consignmentIdCaptor.capture(), testSetUp.getFileMetadataIds.capture(), testSetUp.getFileMetadataPropertyNames.capture()
    )).thenReturn(Future(mockGetFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.addFileMetadata(testSetUp.addFileMetadataCaptor.capture()))
      .thenReturn(Future(mockAddFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.updateFileMetadataProperties(testSetUp.updateFileMetadataPropsArgCaptor.capture()))
      .thenReturn(Future(Seq()))

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.fileUuids, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.fixedUserId
    ).futureValue

    val addFileMetadataArgument = testSetUp.addFileMetadataCaptor.getValue
    val updateFileMetadataIdsArgument: Seq[UUID] = testSetUp.updateFileMetadataPropsArgCaptor.getAllValues.toArray.toSeq.asInstanceOf[Seq[Seq[UUID]]].flatten

    addFileMetadataArgument.length should equal(mockAddFileMetadataResponse.length)
    updateFileMetadataIdsArgument.isEmpty should equal(true)
    updateFileMetadataIdsArgument should equal(mockGetFileMetadataResponse.map(_.metadataid))
  }

  "updateBulkFileMetadata" should "only update metadata rows but not add any" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val (mockGetFileMetadataResponse, _): (Seq[FilemetadataRow], Seq[FilemetadataRow]) =
      generateFileMetadataRows(
        testSetUp.fixedUserId,
        testSetUp.fileUuids,
        testSetUp.filesInFolderFixedFileUuids,
        testSetUp.metadataPropertiesWithOldValues,
        testSetUp.metadataPropertiesWithNewValues,
        Seq("update")
      )

    val fileIdsThatHaveAllProperties =
      mockGetFileMetadataResponse
        .groupBy(_.fileid)
        .filter { case (_, metadataRows) => metadataRows.length == testSetUp.metadataPropertiesWithOldValues.length }
        .keys
        .toSeq

    val fileRowOfFilesThatHaveAllProperties: Seq[FileRow] =
      testSetUp.mockFileResponse.filter(fileRow => fileIdsThatHaveAllProperties.contains(fileRow.fileid))

    when(testSetUp.fileRepositoryMock.getAllDescendants(testSetUp.getDescendentsFileIdsCaptor.capture()))
      .thenReturn(Future(fileRowOfFilesThatHaveAllProperties))
    when(testSetUp.metadataRepositoryMock.getFileMetadata(
      testSetUp.consignmentIdCaptor.capture(), testSetUp.getFileMetadataIds.capture(), testSetUp.getFileMetadataPropertyNames.capture()
    )).thenReturn(Future(mockGetFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.addFileMetadata(testSetUp.addFileMetadataCaptor.capture()))
      .thenReturn(Future(Seq()))
    when(testSetUp.metadataRepositoryMock.updateFileMetadataProperties(testSetUp.updateFileMetadataPropsArgCaptor.capture()))
      .thenReturn(
        Future(Seq("propertyName1", "propertyName2", "propertyName3").map(propertyName => mockGetFileMetadataResponse.count(_.propertyname == propertyName)))
    )

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, fileIdsThatHaveAllProperties, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.fixedUserId
    ).futureValue

    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue

    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap{
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    verify(testSetUp.metadataRepositoryMock, times(1)).addFileMetadata(any[Seq[FilemetadataRow]])
    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(mockGetFileMetadataResponse.map(_.metadataid).sorted)
  }

  "updateBulkFileMetadata" should "not add or update any metadata rows if they already exist" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val (mockGetFileMetadataResponse, _): (Seq[FilemetadataRow], Seq[FilemetadataRow]) =
      generateFileMetadataRows(
        testSetUp.fixedUserId,
        testSetUp.fileUuids,
        testSetUp.filesInFolderFixedFileUuids,
        testSetUp.metadataPropertiesWithOldValues,
        testSetUp.metadataPropertiesWithNewValues,
        Seq()
      )

    val fileIdsThatHaveAllProperties =
      mockGetFileMetadataResponse
        .groupBy(_.fileid)
        .filter { case (_, metadataRows) => metadataRows.length == testSetUp.metadataPropertiesWithOldValues.length }
        .keys
        .toSeq

    val fileRowOfFilesThatHaveAllProperties: Seq[FileRow] =
      testSetUp.mockFileResponse.filter(fileRow => fileIdsThatHaveAllProperties.contains(fileRow.fileid))

    when(testSetUp.fileRepositoryMock.getAllDescendants(testSetUp.getDescendentsFileIdsCaptor.capture()))
      .thenReturn(Future(fileRowOfFilesThatHaveAllProperties))
    when(testSetUp.metadataRepositoryMock.getFileMetadata(
      testSetUp.consignmentIdCaptor.capture(), testSetUp.getFileMetadataIds.capture(), testSetUp.getFileMetadataPropertyNames.capture()
    )).thenReturn(Future(mockGetFileMetadataResponse))
    when(testSetUp.metadataRepositoryMock.addFileMetadata(testSetUp.addFileMetadataCaptor.capture()))
      .thenReturn(Future(Seq()))
    when(testSetUp.metadataRepositoryMock.updateFileMetadataProperties(testSetUp.updateFileMetadataPropsArgCaptor.capture()))
      .thenReturn(Future(Seq()))

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, fileIdsThatHaveAllProperties, testSetUp.metadataPropertiesWithOldValues),
      testSetUp.fixedUserId
    ).futureValue

    verify(testSetUp.metadataRepositoryMock, times(1)).addFileMetadata(any[Seq[FilemetadataRow]])
    verify(testSetUp.metadataRepositoryMock, times(1)).updateFileMetadataProperties(any[Map[String, FileMetadataUpdate]])
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
                                       metadataPropertiesWithOldValues: Seq[UpdateFileMetadataInput],
                                       metadataPropertiesWithNewValues: Seq[UpdateFileMetadataInput],
                                       actionToPerform: Seq[String] = Seq("add", "update")): (Seq[FilemetadataRow], Seq[FilemetadataRow]) = {
    /**
     * Need to create metadata rows to be returned for the getFileMetadata stub. In order to check if the addFileMetadata/updateFileMetadata logic is sound),
     * it's best not to add all metadataProperties for each fileId, so, the number of rows should vary from metadataProperties.length to 1;
     * the rest will be added via the addFileMetadata call (once it's established that they are missing)
     */
    def convertMetadataPropertyIntoFileMetadataRow(fileUuid: UUID)(metadataProperty: UpdateFileMetadataInput): FilemetadataRow =
      FilemetadataRow(UUID.randomUUID(), fileUuid, metadataProperty.value, Timestamp.from(FixedTimeSource.now), fixedUserId, metadataProperty.filePropertyName)

    def generateFileMetadataRowsForAllProperties(fileUuids: Seq[UUID]): Seq[FilemetadataRow] =
      fileUuids.flatMap(fileUuid => metadataPropertiesWithNewValues.map(convertMetadataPropertyIntoFileMetadataRow(fileUuid)))

    def determineHowToDivideMetadataRows(action: Seq[String], index: Int, metadataPropertiesLength: Int): Int =
      // if "add and update" then distribute according to modulo length; else, assign ALL properties to be added or updated to files
      if (action.length == 2) {index % metadataPropertiesLength} else if (action.headOption.contains("add")) {metadataPropertiesLength} else {0}

    val fileMetadataRowsForFirstFileId: Seq[FilemetadataRow] = // 1st id in "fileUuids" is a folder, so instead, generate the metadata based on its file ids
      if (actionToPerform.contains("add")) {generateFileMetadataRowsForAllProperties(filesInFolderFixedFileUuids)} else Nil

    val fileIdsMinusTheFolderId = fileUuids.drop(1)
    val (idsForGetMetadataStub, idsForAddMetadataStub) = fileIdsMinusTheFolderId.splitAt(fileIdsMinusTheFolderId.length / 2)

    val fileMetadataRowsToAdd: Seq[FilemetadataRow] =
      if (actionToPerform.contains("add")) {generateFileMetadataRowsForAllProperties(idsForAddMetadataStub)} else Nil

    val idsForGetMetadataStubZipped: Seq[(UUID, Int)] = idsForGetMetadataStub.zipWithIndex
    val metadataPropertiesLength = (metadataPropertiesWithOldValues.length + metadataPropertiesWithNewValues.length) / 2
    val (remainingMetadataRowsToAdd, metadataRowsForGetFileMetadataStub) = {
      val fileMetadataRowsSplitIntoTwoGroupsPerId: Seq[(Seq[FilemetadataRow], Seq[FilemetadataRow])] =
        idsForGetMetadataStubZipped.map {
          case (fileUuid, index) => // In order to properly test adding/updating, not all files should have every property by default
            val numberOfPropertiesToAddToFile = determineHowToDivideMetadataRows(actionToPerform, index, metadataPropertiesLength)
            val metadataRowsToAdd =
              metadataPropertiesWithNewValues.take(numberOfPropertiesToAddToFile).map(convertMetadataPropertyIntoFileMetadataRow(fileUuid))
            val metadataRowsToUpdate =
              metadataPropertiesWithOldValues.take(metadataPropertiesLength - numberOfPropertiesToAddToFile)
                .map(convertMetadataPropertyIntoFileMetadataRow(fileUuid))

            (metadataRowsToAdd, metadataRowsToUpdate)
        }

      (fileMetadataRowsSplitIntoTwoGroupsPerId.flatMap(_._1), fileMetadataRowsSplitIntoTwoGroupsPerId.flatMap(_._2))
    }
    val metadataRowsForAddFileMetadataStub = fileMetadataRowsForFirstFileId ++ fileMetadataRowsToAdd ++ remainingMetadataRowsToAdd

    (metadataRowsForGetFileMetadataStub, metadataRowsForAddFileMetadataStub)
  }

  private class UpdateBulkMetadataTestSetUp {
    val consignmentId: UUID = UUID.randomUUID()
    val fixedUserId: UUID = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val fixedFolderFileUuid: UUID = UUID.fromString("f89da9b9-4c3b-4a17-a903-61c36b822c17")
    val filesInFolderFixedFileUuids: Seq[UUID] = Seq(UUID.fromString("1a10e744-7292-426b-a36a-a6648152fa35"))
    val fortyNineRandomIds: Seq[UUID] = (0 until 48).map(_ => UUID.randomUUID())
    val fileUuids: Seq[UUID] = fixedFolderFileUuid +: fortyNineRandomIds
    val propertyName1: String = "propertyName1"
    val value1: String = "value1"
    val timestamp: Timestamp = Timestamp.from(FixedTimeSource.now)

    val metadataPropertiesWithOldValues = Seq(
      UpdateFileMetadataInput(propertyName1, value1),
      UpdateFileMetadataInput("propertyName2", "value2"),
      UpdateFileMetadataInput("propertyName3", "value3")
    )

    val metadataPropertiesWithNewValues = Seq(
      UpdateFileMetadataInput(propertyName1, "newValue1"),
      UpdateFileMetadataInput("propertyName2", "newValue2"),
      UpdateFileMetadataInput("propertyName3", "newValue3")
    )

    val metadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
    val fileRepositoryMock: FileRepository = mock[FileRepository]

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val getDescendentsFileIdsCaptor: ArgumentCaptor[Seq[UUID]] = ArgumentCaptor.forClass(classOf[Seq[UUID]])
    val getFileMetadataIds: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val getFileMetadataPropertyNames: ArgumentCaptor[Option[Set[String]]] = ArgumentCaptor.forClass(classOf[Option[Set[String]]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val updateFileMetadataPropsArgCaptor: ArgumentCaptor[Map[String, FileMetadataUpdate]] = ArgumentCaptor.forClass(classOf[Map[String, FileMetadataUpdate]])

    val mockFileResponse: Seq[FileRow] = generateFileRows(fileUuids, filesInFolderFixedFileUuids, fixedUserId, timestamp)
  }
}
