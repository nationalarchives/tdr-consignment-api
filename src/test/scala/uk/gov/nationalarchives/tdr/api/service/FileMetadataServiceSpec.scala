package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileMetadataUpdate, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Failed, Mismatch, Success}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addFileMetadata" should "call the metadata repository with the correct row arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
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

    when(metadataRepositoryMock.addChecksumMetadata(addChecksumCaptor.capture(), any[Seq[FilestatusRow]]))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(getFileMetadataFileCaptor.capture(), getFileMetadataPropertyNameCaptor.capture()))
      .thenReturn(Future(mockFileMetadataResponse))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
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
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileId = UUID.randomUUID()

    when(metadataRepositoryMock.getFileMetadataByProperty(fileId, SHA256ClientSideChecksum))
      .thenReturn(Future(Seq()))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val err =
      service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fileId, "value"), UUID.randomUUID()).failed.futureValue

    err.getMessage should equal(s"Could not find metadata for file $fileId")
  }

  "addFileMetadata" should "call the metadata repository with the correct arguments if the checksum matches" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
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

    val fileStatusCaptor: ArgumentCaptor[Seq[FilestatusRow]] = ArgumentCaptor.forClass(classOf[Seq[FilestatusRow]])
    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)

    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fixedFileUuid, "ChecksumMatch"), fixedUserId).futureValue
    fileStatusCaptor.getValue.size should equal(2)
    fileStatusCaptor.getValue.head.fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue.head.statustype should equal("ChecksumMatch")
    fileStatusCaptor.getValue.head.value should equal(Success)
    fileStatusCaptor.getValue()(1).fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue()(1).statustype should equal("ServerChecksum")
    fileStatusCaptor.getValue()(1).value should equal(Success)
  }

  "addFileMetadata" should "call the metadata repository with the correct arguments if the checksum doesn't match" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
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

    val fileStatusCaptor: ArgumentCaptor[Seq[FilestatusRow]] = ArgumentCaptor.forClass(classOf[Seq[FilestatusRow]])
    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(AddFileMetadataWithFileIdInput(SHA256ServerSideChecksum, fixedFileUuid, ""), fixedUserId).futureValue
    fileStatusCaptor.getValue.size should equal(2)
    fileStatusCaptor.getValue.head.fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue.head.statustype should equal("ChecksumMatch")
    fileStatusCaptor.getValue.head.value should equal(Mismatch)
    fileStatusCaptor.getValue()(1).fileid should equal(fixedFileUuid)
    fileStatusCaptor.getValue()(1).statustype should equal("ServerChecksum")
    fileStatusCaptor.getValue()(1).value should equal(Failed)
  }

  "addFileMetadata" should "return the correct data for a single update" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val value = "value"
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
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

    when(metadataRepositoryMock.addChecksumMetadata(any[FilemetadataRow], any[Seq[FilestatusRow]])).thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    val result: FileMetadataWithFileId =
      service.addFileMetadata(AddFileMetadataWithFileIdInput(propertyName, fixedFileUuid, "value"), fixedUserId).futureValue

    result.fileId should equal(fixedFileUuid)
    result.filePropertyName should equal(propertyName)
    result.value should equal(value)
  }

  "addFileMetadata" should "fail if the update is not for a checksum" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileId = UUID.randomUUID()

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val err = service.addFileMetadata(AddFileMetadataWithFileIdInput("SomethingElse", fileId, "ChecksumMatch"), UUID.randomUUID()).failed.futureValue
    err.getMessage should equal("SomethingElse found. We are only expecting checksum updates for now")
  }

  "updateBulkFileMetadata" should "pass the correct number of ids into getAllDescendants if there are duplicates present in input argument" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    testSetUp.stubRepoResponses()

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    val duplicateInputFileIds = testSetUp.inputFileIds ++ testSetUp.inputFileIds
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, duplicateInputFileIds, testSetUp.metadataPropertiesWithNewValues), testSetUp.userId
    ).futureValue

    val getDescendentsFileIdsArgument: Seq[UUID] = testSetUp.getDescendentsFileIdsCaptor.getValue
    getDescendentsFileIdsArgument should equal(testSetUp.inputFileIds)
  }

  "updateBulkFileMetadata" should "only pass the file Ids of the folder, into getFileMetadata if a folder Id was passed as in an input Id" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)
    val existingFileMetadataRow =
      Seq(FilemetadataRow(UUID.randomUUID(), UUID.randomUUID(), "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"))

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRow)

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues), testSetUp.userId
    ).futureValue

    val fileIdsArgument: Option[Set[UUID]] = testSetUp.getFileMetadataIds.getValue
    val fileIdsPassedIn: Set[UUID] = (testSetUp.inputFileIds.drop(1) ++ testSetUp.folderAndItsFiles.drop(1)).toSet

    fileIdsArgument.getOrElse(Set()) should equal(fileIdsPassedIn)
  }

  "updateBulkFileMetadata" should "pass the correct consignment Id and property names to getFileMetadata" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)
    val existingFileMetadataRow =
      Seq(FilemetadataRow(UUID.randomUUID(), UUID.randomUUID(), "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"))

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRow)

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues), testSetUp.userId
    ).futureValue

    val consignmentIdArgument: UUID = testSetUp.consignmentIdCaptor.getValue
    val propertyNameArguments: Option[Set[String]] = testSetUp.getFileMetadataPropertyNames.getValue

    consignmentIdArgument should equal(testSetUp.consignmentId)
    propertyNameArguments.get should equal(testSetUp.metadataPropertiesWithNewValues.map(_.filePropertyName).toSet)
  }

  "updateBulkFileMetadata" should "add metadata rows that haven't already been added and update those that have" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)
    val existingFileMetadataRows = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    val mockAddFileMetadataResponse = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRows, mockAddFileMetadataResponse, Seq(existingFileMetadataRows.length))

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.userId
    ).futureValue

    val addFileMetadataArgument = testSetUp.addFileMetadataCaptor.getValue
    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue
    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap {
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    addFileMetadataArgument.nonEmpty should equal(true)
    addFileMetadataArgument.map(_.fileid).sorted should equal(mockAddFileMetadataResponse.map(_.fileid).toVector.sorted)
    addFileMetadataArgument.forall {
      metadataRow => metadataRow.value.startsWith("newValue") && metadataRow.propertyname.last == metadataRow.value.last
    } should equal(true)

    // metadata Ids that go into updateFileMetadata should be the same as the ones that were taken from the getFileMetadata
    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataArgument.forall {
      case (propertyName, metadataRow) => metadataRow.value.startsWith("newValue") && propertyName.last == metadataRow.value.last
    } should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(existingFileMetadataRows.map(_.metadataid).sorted)
  }

  "updateBulkFileMetadata" should "pass into 'updateFileMetadata', only the metadataIds where the " +
    "'value' (of its FileMetadata row) differs from the value the user is trying to set" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)
    val existingFileMetadataRows =
      Seq(
        FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
        FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
        FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
        FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
      )

    // leave out propertyName1 because it would not have been updated
    val numberOfRowsWithPropertyName = Seq("propertyName2").map(propertyName => existingFileMetadataRows.count(_.propertyname == propertyName))

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRows, Seq(), numberOfRowsWithPropertyName)

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    val newMetadataPropertiesWithOneOldValue = Seq(
      UpdateFileMetadataInput("propertyName1", "value1"), // this value already exists on propertyName1, therefore, there is no need to update it
      UpdateFileMetadataInput("propertyName2", "newValue2")
    )
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, newMetadataPropertiesWithOneOldValue),
      testSetUp.userId
    ).futureValue

    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue

    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap {
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    val metadataIdsWithoutOldValue: Seq[UUID] = existingFileMetadataRows.collect {
      case fileMetadataRow if fileMetadataRow.value != "value1" => fileMetadataRow.metadataid
    }

    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(metadataIdsWithoutOldValue.sorted)
  }

  "updateBulkFileMetadata" should "only add metadata rows but not update any" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)

    val mockAddFileMetadataResponse = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    testSetUp.stubRepoResponses(existingFileRows, Seq(), mockAddFileMetadataResponse)

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.userId
    ).futureValue

    val addFileMetadataArgument: Seq[FilemetadataRow] = testSetUp.addFileMetadataCaptor.getValue

    verify(testSetUp.metadataRepositoryMock, times(1)).updateFileMetadataProperties(any[Map[String, FileMetadataUpdate]])
    testSetUp.updateFileMetadataPropsArgCaptor.getValue should equal(Map())

    addFileMetadataArgument.nonEmpty should equal(true)
    addFileMetadataArgument.length should equal(10)
    addFileMetadataArgument.forall {
      metadataRow => metadataRow.value.startsWith("newValue") && metadataRow.propertyname.last == metadataRow.value.last
    } should equal(true)
  }

  "updateBulkFileMetadata" should "only update metadata rows but not add any" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)
    val existingFileMetadataRows = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "value2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRows, Seq(), Seq(existingFileMetadataRows.length))

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.userId
    ).futureValue

    val updateFileMetadataArgument: Map[String, FileMetadataUpdate] = testSetUp.updateFileMetadataPropsArgCaptor.getValue

    val updateFileMetadataIdsArgument: Seq[UUID] = updateFileMetadataArgument.toSeq.flatMap {
      case (_, fileMetadataUpdate) => fileMetadataUpdate.metadataIds
    }

    verify(testSetUp.metadataRepositoryMock, times(1)).addFileMetadata(any[Seq[FilemetadataRow]])
    testSetUp.addFileMetadataCaptor.getValue should equal(Seq())

    updateFileMetadataIdsArgument.nonEmpty should equal(true)
    updateFileMetadataIdsArgument.sorted should equal(existingFileMetadataRows.map(_.metadataid).sorted)
    updateFileMetadataArgument.forall {
      case (propertyName, metadataRow) => metadataRow.value.startsWith("newValue") && propertyName.last == metadataRow.value.last
    } should equal(true)
  }

  "updateBulkFileMetadata" should "not add or update any metadata rows if they already exist" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.folderAndItsFiles, testSetUp.inputFileIds, testSetUp.userId)

    val existingFileMetadataRows = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileInFolderId3, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    testSetUp.stubRepoResponses(existingFileRows, existingFileMetadataRows, Seq(), Seq())

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, testSetUp.fileRepositoryMock, testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource, testSetUp.fixedUUIDSource)
    service.updateBulkFileMetadata(
      UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.metadataPropertiesWithNewValues),
      testSetUp.userId
    ).futureValue

    testSetUp.addFileMetadataCaptor.getValue should equal(Seq())
    testSetUp.updateFileMetadataPropsArgCaptor.getValue should equal(Map())

    verify(testSetUp.metadataRepositoryMock, times(1)).addFileMetadata(any[Seq[FilemetadataRow]])
    verify(testSetUp.metadataRepositoryMock, times(1)).updateFileMetadataProperties(any[Map[String, FileMetadataUpdate]])
  }

  "getFileMetadata" should "call the repository with the correct arguments" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()
    val mockResponse = Future(Seq())

    when(fileMetadataRepositoryMock.getFileMetadata(
      consignmentIdCaptor.capture(), selectedFileIdsCaptor.capture(), any[Option[Set[String]]]
    )).thenReturn(mockResponse)

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    service.getFileMetadata(consignmentId).futureValue
    consignmentIdCaptor.getValue should equal(consignmentId)
    selectedFileIdsCaptor.getValue should equal(None)
  }

  "getFileMetadata" should "return multiple map entries for multiple files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val consignmentId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val closureStartDate = Timestamp.from(Instant.parse("2020-01-01T09:00:00Z"))
    val foiExemptionAsserted = Timestamp.from(Instant.parse("2020-01-01T09:00:00Z"))
    val mockResponse = Future(Seq(
      FilemetadataRow(UUID.randomUUID(), fileIdOne, "1", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "ClientSideFileSize"),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, "valueTwo", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "FoiExemptionCode"),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, closureStartDate.toString, Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), ClosureStartDate),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, foiExemptionAsserted.toString, Timestamp.from(FixedTimeSource.now),
        UUID.randomUUID(), FoiExemptionAsserted),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, "true", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), TitlePublic),
      FilemetadataRow(UUID.randomUUID(), fileIdTwo, "1", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), ClosurePeriod)
    ))

    when(fileMetadataRepositoryMock.getFileMetadata(any[UUID], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(mockResponse)

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.getFileMetadata(consignmentId).futureValue

    response.size should equal(2)
    response.contains(fileIdOne) should equal(true)
    response.contains(fileIdTwo) should equal(true)
    response(fileIdOne).clientSideFileSize.get should equal(1)
    response(fileIdTwo).foiExemptionCode.get should equal("valueTwo")
    response(fileIdTwo).closureStartDate.get should equal(closureStartDate.toLocalDateTime)
    response(fileIdTwo).closurePeriod.get should equal(1)
    response(fileIdTwo).titlePublic.get should equal(true)
    response(fileIdTwo).foiExemptionAsserted.get should equal(foiExemptionAsserted.toLocalDateTime)
  }

  private def generateFileRows(fileUuids: Seq[UUID], filesInFolderFixedFileUuids: Seq[UUID], fixedUserId: UUID): Seq[FileRow] = {
    val consignmentId = UUID.randomUUID()
    val timestamp: Timestamp = Timestamp.from(FixedTimeSource.now)

    val folderFileRow = Seq(
      FileRow(
        fileUuids.head, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName")
      )
    )

    val fileRowsForFilesInFolder: Seq[FileRow] = filesInFolderFixedFileUuids.drop(1).map(fileUuid =>
      FileRow(
        fileUuid, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileName"), Some(fileUuids.head)
      )
    )

    val folderAndFileRows = folderFileRow ++ fileRowsForFilesInFolder

    val fileRowsExceptFirst: Seq[FileRow] = fileUuids.drop(1).map(fileUuid =>
      FileRow(
        fileUuid, consignmentId, fixedUserId, timestamp, Some(true), Some(NodeType.fileTypeIdentifier), Some("fileName")
      )
    )

    folderAndFileRows ++ fileRowsExceptFirst
  }

  private class UpdateBulkMetadataTestSetUp {
    val userId: UUID = UUID.randomUUID()
    val consignmentId: UUID = UUID.randomUUID()
    val folderId: UUID = UUID.fromString("f89da9b9-4c3b-4a17-a903-61c36b822c17")
    val fileInFolderId1: UUID = UUID.randomUUID()
    val fileInFolderId2: UUID = UUID.randomUUID()
    val fileInFolderId3: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()
    val folderAndItsFiles = Seq(folderId, fileInFolderId1, fileInFolderId2, fileInFolderId3)
    val inputFileIds = Seq(folderId, fileId1, fileId2)

    val propertyName1: String = "propertyName1"

    val metadataPropertiesWithNewValues = Seq(
      UpdateFileMetadataInput(propertyName1, "newValue1"),
      UpdateFileMetadataInput("propertyName2", "newValue2")
    )

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val metadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
    val fileRepositoryMock: FileRepository = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock: CustomMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val getDescendentsFileIdsCaptor: ArgumentCaptor[Seq[UUID]] = ArgumentCaptor.forClass(classOf[Seq[UUID]])
    val getFileMetadataIds: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val getFileMetadataPropertyNames: ArgumentCaptor[Option[Set[String]]] = ArgumentCaptor.forClass(classOf[Option[Set[String]]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val updateFileMetadataPropsArgCaptor: ArgumentCaptor[Map[String, FileMetadataUpdate]] = ArgumentCaptor.forClass(classOf[Map[String, FileMetadataUpdate]])

    def stubRepoResponses(getAllDescendantsResponse: Seq[FileRow] = Seq(), getFileMetadataResponse: Seq[FilemetadataRow] = Seq(),
                          addFileMetadataResponse: Seq[FilemetadataRow] = Seq(), updateFileMetadataPropertiesResponse: Seq[Int] = Seq()): Unit = {

      when(fileRepositoryMock.getAllDescendants(getDescendentsFileIdsCaptor.capture())).thenReturn(Future(getAllDescendantsResponse))
      when(metadataRepositoryMock.getFileMetadata(
        consignmentIdCaptor.capture(), getFileMetadataIds.capture(), getFileMetadataPropertyNames.capture())
      ).thenReturn(Future(getFileMetadataResponse))
      when(metadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture()))
        .thenReturn(Future(addFileMetadataResponse))
      when(metadataRepositoryMock.updateFileMetadataProperties(updateFileMetadataPropsArgCaptor.capture()))
        .thenReturn(Future(updateFileMetadataPropertiesResponse))
      ()
    }
  }
}
