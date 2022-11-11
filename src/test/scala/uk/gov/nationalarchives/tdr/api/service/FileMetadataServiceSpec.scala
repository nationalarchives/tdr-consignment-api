package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilepropertydependenciesRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileMetadataUpdate, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Failed, Mismatch, Success}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

//scalastyle:off file.size.limit
class FileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addFileMetadata" should "call the metadata repository with the correct row arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
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
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum)
    )
    val propertyName = SHA256ServerSideChecksum
    val fixedUUIDSource = new FixedUUIDSource()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
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

  "updateBulkFileMetadata" should "delete existing metadata rows and add new metadata rows based on the input" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.inputFileIds, testSetUp.folderAndChildrenIds, testSetUp.userId)

    val mockAddFileMetadataResponse = Seq(
      FilemetadataRow(UUID.randomUUID(), testSetUp.childFileId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.fileId1, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.childFileId2, "newValue1", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName1"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.childFileId2, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2"),
      FilemetadataRow(UUID.randomUUID(), testSetUp.childFileId3, "newValue2", Timestamp.from(FixedTimeSource.now), testSetUp.userId, "propertyName2")
    )

    testSetUp.stubRepoResponses(existingFileRows, 0, mockAddFileMetadataResponse)

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.fileRepositoryMock,
      testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource,
      testSetUp.fixedUUIDSource
    )

    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.newMetadataProperties)

    service.updateBulkFileMetadata(input, testSetUp.userId).futureValue
    val deleteFileMetadataIdsArg: Set[UUID] = testSetUp.deleteFileMetadataIdsArgCaptor.getValue
    val deleteFileMetadataPropertiesArg: Set[String] = testSetUp.deletePropertyNamesCaptor.getValue
    val addFileMetadataArgument: Seq[FilemetadataRow] = testSetUp.addFileMetadataCaptor.getValue

    val expectedDeleteFileIds: Set[UUID] = (testSetUp.inputFileIds.drop(1) ++ testSetUp.folderAndChildrenIds.drop(1)).toSet
    val expectedDeletedPropertiesArg: Set[String] = Set("propertyName1", "propertyName2")

    deleteFileMetadataIdsArg should equal(expectedDeleteFileIds)
    deleteFileMetadataPropertiesArg should equal(expectedDeletedPropertiesArg)
    addFileMetadataArgument.size should equal(10)
    val addedFileIds = addFileMetadataArgument.map(_.fileid).toSet
    addedFileIds.size should equal(deleteFileMetadataIdsArg.size)
    addedFileIds.subsetOf(expectedDeleteFileIds) should equal(true)

    val addedPropertyValues = addFileMetadataArgument.map(_.value).toSet
    addedPropertyValues.size should equal(2)
    addedPropertyValues.subsetOf(Set("newValue1", "newValue2")) should equal(true)

    val addedProperties = addFileMetadataArgument.map(_.propertyname).toSet
    addedProperties.size should equal(2)
    addedProperties.subsetOf(Set("propertyName1", "propertyName2")) should equal(true)
  }

  "updateBulkFileMetadata" should "pass the correct number of ids into getAllDescendants if there are duplicates present in input argument" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    testSetUp.stubRepoResponses()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.fileRepositoryMock,
      testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource,
      testSetUp.fixedUUIDSource
    )
    val duplicateInputFileIds = testSetUp.inputFileIds ++ testSetUp.inputFileIds
    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, duplicateInputFileIds, testSetUp.newMetadataProperties)
    service.updateBulkFileMetadata(input, testSetUp.userId).futureValue

    val getDescendentsFileIdsArgument: Seq[UUID] = testSetUp.getAllDescendentIdsCaptor.getValue
    getDescendentsFileIdsArgument should equal(testSetUp.inputFileIds)
  }

  "updateBulkFileMetadata" should "only delete children file Ids if a folder Id was passed as in as input" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()

    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.inputFileIds, testSetUp.folderAndChildrenIds, testSetUp.userId)

    testSetUp.stubRepoResponses(existingFileRows)

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.fileRepositoryMock,
      testSetUp.customMetadataPropertiesRepositoryMock,
      FixedTimeSource,
      testSetUp.fixedUUIDSource
    )
    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.newMetadataProperties)

    service.updateBulkFileMetadata(input, testSetUp.userId).futureValue

    val fileIdsArgument: Set[UUID] = testSetUp.deleteFileMetadataIdsArgCaptor.getValue
    val fileIdsPassedIn: Set[UUID] = (testSetUp.inputFileIds.drop(1) ++ testSetUp.folderAndChildrenIds.drop(1)).toSet

    fileIdsArgument should equal(fileIdsPassedIn)
  }

  "getFileMetadata" should "call the repository with the correct arguments" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()
    val mockResponse = Future(Seq())

    when(
      fileMetadataRepositoryMock.getFileMetadata(
        consignmentIdCaptor.capture(),
        selectedFileIdsCaptor.capture(),
        any[Option[Set[String]]]
      )
    ).thenReturn(mockResponse)

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
    val mockResponse = Future(
      Seq(
        FilemetadataRow(UUID.randomUUID(), fileIdOne, "1", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "ClientSideFileSize"),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, "valueTwo", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), "FoiExemptionCode"),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, closureStartDate.toString, Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), ClosureStartDate),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, foiExemptionAsserted.toString, Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), FoiExemptionAsserted),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, "true", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), TitleClosed),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, "true", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), DescriptionClosed),
        FilemetadataRow(UUID.randomUUID(), fileIdTwo, "1", Timestamp.from(FixedTimeSource.now), UUID.randomUUID(), ClosurePeriod)
      )
    )

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
    response(fileIdTwo).titleClosed.get should equal(true)
    response(fileIdTwo).descriptionClosed.get should equal(true)
    response(fileIdTwo).foiExemptionAsserted.get should equal(foiExemptionAsserted.toLocalDateTime)
  }

  "getSumOfFileSizes" should "return the sum of the file sizes" in {
    val fileMetadataRepository = mock[FileMetadataRepository]
    val consignmentId = UUID.randomUUID()
    when(fileMetadataRepository.getSumOfFileSizes(any[UUID])).thenReturn(Future(1))

    val service = new FileMetadataService(fileMetadataRepository, mock[FileRepository], mock[CustomMetadataPropertiesRepository], FixedTimeSource, new FixedUUIDSource())
    val result = service.getSumOfFileSizes(consignmentId).futureValue

    result should equal(1)
    verify(fileMetadataRepository, times(1)).getSumOfFileSizes(consignmentId)
  }

  "deleteFileMetadata" should "delete and update fileMetadata properties with a default value for the selected files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)

    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosureType", "Closed", None, Some(3), None, None),
        FilepropertyvaluesRow("ClosureType", "Open", Some(true), Some(1), None, None),
        FilepropertyvaluesRow("TitleClosed", "true", None, Some(2), None, None),
        FilepropertyvaluesRow("TitleClosed", "false", Some(true), Some(1), None, None),
        FilepropertyvaluesRow("Property1", "33", None, Some(1), None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None),
        FilepropertydependenciesRow(3, "ClosureStartDate", None),
        FilepropertydependenciesRow(3, "TitleClosed", None),
        FilepropertydependenciesRow(2, "Property1", None)
      )
    )

    val fileMetadataUpdateCaptor: ArgumentCaptor[Map[String, FileMetadataUpdate]] = ArgumentCaptor.forClass(classOf[Map[String, FileMetadataUpdate]])
    val expectedPropertyNamesToDelete = Set("ClosurePeriod", "ClosureStartDate")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), ArgumentMatchers.eq(expectedPropertyNamesToDelete))).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.updateFileMetadataProperties(ArgumentMatchers.eq(fileIds.toSet), fileMetadataUpdateCaptor.capture())).thenReturn(Future(Nil))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId)), userId).futureValue

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete.toSeq ++ Seq("TitleClosed"))
    val fileMetadataUpdate = fileMetadataUpdateCaptor.getValue
    fileMetadataUpdate.size should equal(2)
    fileMetadataUpdate.head._1 should equal(TitleClosed)
    fileMetadataUpdate.head._2.value should equal("false")
    fileMetadataUpdate.head._2.filePropertyName should equal(TitleClosed)
    fileMetadataUpdate.head._2.userId should equal(userId)
    fileMetadataUpdate.head._2.dateTime != null shouldBe true
    fileMetadataUpdate.last._1 should equal(ClosureType)
    fileMetadataUpdate.last._2.value should equal("Open")
    fileMetadataUpdate.last._2.filePropertyName should equal(ClosureType)
    fileMetadataUpdate.last._2.userId should equal(userId)
    fileMetadataUpdate.last._2.dateTime != null shouldBe true
  }

  "deleteFileMetadata" should "update fileMetadata properties only if all of the properties have a default value" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)

    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosureType", "Closed", None, Some(3), None, None),
        FilepropertyvaluesRow("ClosureType", "Open", Some(true), Some(1), None, None),
        FilepropertyvaluesRow("TitleClosed", "true", None, Some(2), None, None),
        FilepropertyvaluesRow("TitleClosed", "false", Some(true), Some(1), None, None),
        FilepropertyvaluesRow("Property1", "33", None, Some(1), None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "TitleClosed", None),
        FilepropertydependenciesRow(2, "Property1", None)
      )
    )

    val fileMetadataUpdateCaptor: ArgumentCaptor[Map[String, FileMetadataUpdate]] = ArgumentCaptor.forClass(classOf[Map[String, FileMetadataUpdate]])
    val expectedPropertyNamesToDelete: Set[String] = Set()

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    when(fileMetadataRepositoryMock.updateFileMetadataProperties(ArgumentMatchers.eq(fileIds.toSet), fileMetadataUpdateCaptor.capture())).thenReturn(Future(Nil))
    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), ArgumentMatchers.eq(Set()))).thenReturn(Future(2))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId)), userId).futureValue

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete.toSeq ++ Seq("TitleClosed"))
    val fileMetadataUpdate = fileMetadataUpdateCaptor.getValue
    fileMetadataUpdate.size should equal(2)
    fileMetadataUpdate.head._1 should equal(TitleClosed)
    fileMetadataUpdate.last._1 should equal(ClosureType)
  }

  "deleteFileMetadata" should "throw an exception if a CustomMetadata property is missing in the db" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)

    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosureType", "Open", None, Some(1), None, None),
        FilepropertyvaluesRow("TitlePublic", "ABC", None, Some(1), None, None)
      )
    )
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    verify(fileMetadataRepositoryMock, times(0)).updateFileMetadataProperties(any[Set[UUID]], any[Map[String, FileMetadataUpdate]])
    verify(fileMetadataRepositoryMock, times(0)).deleteFileMetadata(any[Set[UUID]], any[Set[String]])
    verify(customMetadataPropertiesRepositoryMock, times(0)).getCustomMetadataDependencies

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val thrownException = intercept[Exception] {
      service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId)), UUID.randomUUID()).futureValue
    }

    thrownException.getMessage should include("Can't find metadata property 'ClosureType' with value 'Closed' in the db.")
  }

  private def generateFileRows(fileUuids: Seq[UUID], filesInFolderFixedFileUuids: Seq[UUID], fixedUserId: UUID): Seq[FileRow] = {
    val consignmentId = UUID.randomUUID()
    val timestamp: Timestamp = Timestamp.from(FixedTimeSource.now)

    val folderFileRow = Seq(
      FileRow(
        fileUuids.head,
        consignmentId,
        fixedUserId,
        timestamp,
        Some(true),
        Some(directoryTypeIdentifier),
        Some("folderName")
      )
    )

    val fileRowsForFilesInFolder: Seq[FileRow] = filesInFolderFixedFileUuids
      .drop(1)
      .map(fileUuid =>
        FileRow(
          fileUuid,
          consignmentId,
          fixedUserId,
          timestamp,
          Some(true),
          Some(fileTypeIdentifier),
          Some("fileName"),
          Some(fileUuids.head)
        )
      )

    val folderAndFileRows = folderFileRow ++ fileRowsForFilesInFolder

    val fileRowsExceptFirst: Seq[FileRow] = fileUuids
      .drop(1)
      .map(fileUuid =>
        FileRow(
          fileUuid,
          consignmentId,
          fixedUserId,
          timestamp,
          Some(true),
          Some(fileTypeIdentifier),
          Some("fileName")
        )
      )

    folderAndFileRows ++ fileRowsExceptFirst
  }

  private class UpdateBulkMetadataTestSetUp {
    val userId: UUID = UUID.randomUUID()
    val consignmentId: UUID = UUID.randomUUID()
    val folderId: UUID = UUID.fromString("f89da9b9-4c3b-4a17-a903-61c36b822c17")
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()
    val childFileId1: UUID = UUID.randomUUID()
    val childFileId2: UUID = UUID.randomUUID()
    val childFileId3: UUID = UUID.randomUUID()

    val folderAndChildrenIds: Seq[UUID] = Seq(folderId, childFileId1, childFileId2, childFileId3)
    val inputFileIds: Seq[UUID] = Seq(folderId, fileId1, fileId2)

    val propertyName1: String = "propertyName1"

    val newMetadataProperties: Seq[UpdateFileMetadataInput] = Seq(
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, propertyName1, "newValue1"),
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "propertyName2", "newValue2")
    )

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val metadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
    val fileRepositoryMock: FileRepository = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock: CustomMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val getAllDescendentIdsCaptor: ArgumentCaptor[Seq[UUID]] = ArgumentCaptor.forClass(classOf[Seq[UUID]])
    val deletePropertyNamesCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val deleteFileMetadataIdsArgCaptor: ArgumentCaptor[Set[UUID]] = ArgumentCaptor.forClass(classOf[Set[UUID]])

    def stubRepoResponses(
        getAllDescendantsResponse: Seq[FileRow] = Seq(),
        deleteFileMetadataResponse: Int = 0,
        addFileMetadataResponse: Seq[FilemetadataRow] = Seq()
    ): Unit = {

      when(fileRepositoryMock.getAllDescendants(getAllDescendentIdsCaptor.capture())).thenReturn(Future(getAllDescendantsResponse))
      when(metadataRepositoryMock.deleteFileMetadata(deleteFileMetadataIdsArgCaptor.capture(), deletePropertyNamesCaptor.capture()))
        .thenReturn(Future(deleteFileMetadataResponse))
      when(metadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture()))
        .thenReturn(Future(addFileMetadataResponse))
    }
  }
}
