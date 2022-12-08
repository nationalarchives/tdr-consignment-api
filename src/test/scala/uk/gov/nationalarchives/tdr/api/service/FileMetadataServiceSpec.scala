package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{SHA256ServerSideChecksum, _}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{directoryTypeIdentifier, fileTypeIdentifier}
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val fixedUUIDSource = new FixedUUIDSource()
    val metadataId: UUID = fixedUUIDSource.uuid
    fixedUUIDSource.reset

    val addChecksumCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])
    val getFileMetadataFileCaptor: ArgumentCaptor[List[UUID]] = ArgumentCaptor.forClass(classOf[List[UUID]])
    val getFileMetadataPropertyNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val mockFileMetadataResponse =
      Seq(FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ClientSideChecksum))

    when(metadataRepositoryMock.addChecksumMetadata(addChecksumCaptor.capture(), any[Seq[FilestatusRow]]))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(getFileMetadataFileCaptor.capture(), getFileMetadataPropertyNameCaptor.capture()))
      .thenReturn(Future(mockFileMetadataResponse))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(createInput(SHA256ServerSideChecksum, fixedFileUuid, "value"), fixedUserId).futureValue

    val row = addChecksumCaptor.getValue.head
    row.propertyname should equal(SHA256ServerSideChecksum)
    row.fileid should equal(fixedFileUuid)
    row.userid should equal(fixedUserId)
    row.datetime should equal(Timestamp.from(FixedTimeSource.now))
    row.metadataid.shouldBe(metadataId)
  }

  def createInput(propertyName: String, fileId: UUID, value: String): AddFileMetadataWithFileIdInput = {
    AddFileMetadataWithFileIdInput(AddFileMetadataWithFileIdInputValues(propertyName, fileId, value) :: Nil)
  }

  "addFileMetadata" should "call throw an error if the client side checksum is missing" in {
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileId = UUID.randomUUID()

    when(metadataRepositoryMock.getFileMetadataByProperty(fileId :: Nil, SHA256ClientSideChecksum))
      .thenReturn(Future(Seq()))
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val err =
      service.addFileMetadata(createInput(SHA256ServerSideChecksum, fileId, "value"), UUID.randomUUID()).failed.futureValue

    err.getMessage should equal(s"Cannot find client side checksum for file $fileId")
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val fileStatusCaptor: ArgumentCaptor[Seq[FilestatusRow]] = ArgumentCaptor.forClass(classOf[Seq[FilestatusRow]])
    when(metadataRepositoryMock.addChecksumMetadata(any[List[FilemetadataRow]], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)

    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid :: Nil, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)
    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(createInput(SHA256ServerSideChecksum, fixedFileUuid, "ChecksumMatch"), fixedUserId).futureValue
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val fileStatusCaptor: ArgumentCaptor[Seq[FilestatusRow]] = ArgumentCaptor.forClass(classOf[Seq[FilestatusRow]])
    when(metadataRepositoryMock.addChecksumMetadata(any[List[FilemetadataRow]], fileStatusCaptor.capture()))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid :: Nil, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    service.addFileMetadata(createInput(SHA256ServerSideChecksum, fixedFileUuid, ""), fixedUserId).futureValue
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
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val propertyName = SHA256ServerSideChecksum
    val fixedUUIDSource = new FixedUUIDSource()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))

    when(metadataRepositoryMock.addChecksumMetadata(any[List[FilemetadataRow]], any[Seq[FilestatusRow]])).thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid :: Nil, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, fixedUUIDSource)
    val result: FileMetadataWithFileId =
      service.addFileMetadata(createInput(propertyName, fixedFileUuid, "value"), fixedUserId).futureValue.head

    result.fileId should equal(fixedFileUuid)
    result.filePropertyName should equal(propertyName)
    result.value should equal(value)
  }

  "updateBulkFileMetadata" should "delete existing metadata rows and add new metadata rows based on the input" in {
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
    val deleteFileMetadataIdsArg: Set[UUID] = testSetUp.deleteFileMetadataIdsArgCaptor.getValue
    val deleteFileMetadataPropertiesArg: Set[String] = testSetUp.deletePropertyNamesCaptor.getValue
    val addFileMetadataArgument: Seq[FilemetadataRow] = testSetUp.addFileMetadataCaptor.getValue

    val expectedUpdatedIds: Set[UUID] = Set(testSetUp.fileId1, testSetUp.childFileId1, testSetUp.childFileId2)
    val expectedUpdatedPropertyNames: Set[String] = Set("propertyName1", "propertyName2", "propertyName3")
    val expectedUpdatedPropertyValues: Set[String] = Set("newValue1", "newValue2", "newValue3", "newValue4")

    deleteFileMetadataIdsArg should equal(expectedUpdatedIds)
    deleteFileMetadataPropertiesArg should equal(expectedUpdatedPropertyNames)

    addFileMetadataArgument.size should equal(12)
    val addedFileIds = addFileMetadataArgument.map(_.fileid).toSet
    addedFileIds.size should equal(deleteFileMetadataIdsArg.size)
    addedFileIds.subsetOf(expectedUpdatedIds) should equal(true)

    val addedPropertyValues = addFileMetadataArgument.map(_.value).toSet
    addedPropertyValues.size should equal(4)
    addedPropertyValues.subsetOf(expectedUpdatedPropertyValues) should equal(true)

    val addedProperties = addFileMetadataArgument.map(_.propertyname).toSet
    addedProperties.size should equal(3)
    addedProperties.subsetOf(expectedUpdatedPropertyNames) should equal(true)
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

  "deleteFileMetadata" should "throw an exception if property name does not exist" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )

    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosureType", "Open", None, Some(1), None, None),
        FilepropertyvaluesRow("TitlePublic", "ABC", None, Some(1), None, None)
      )
    )

    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "TitleClosed", None),
        FilepropertydependenciesRow(2, "Property1", None)
      )
    )
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val thrownException = intercept[Exception] {
      service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId), Seq(FileMetadataToDelete("Non-ExistentProperty", None))), UUID.randomUUID()).futureValue
    }

    verify(fileMetadataRepositoryMock, times(0)).addFileMetadata(any[Seq[FilemetadataRow]])
    verify(fileMetadataRepositoryMock, times(0)).deleteFileMetadata(any[Set[UUID]], any[Set[String]])
    verify(customMetadataPropertiesRepositoryMock, times(1)).getCustomMetadataDependencies

    thrownException.getMessage should include("'Non-ExistentProperty' is not an existing property.")
  }

  "deleteFileMetadata" should "be able to handle requests to delete and/or reset multiple top-level properties (whether they have dependencies or not)" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("TitleClosed", None, Some("Title Closed"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("TitleClosedGroup")),
        FilepropertyRow("Property1", None, Some("Property 1"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Property1Group")),
        FilepropertyRow(
          "TopLevelProperty1",
          None,
          Some("Top Level Property 1"),
          Timestamp.from(Instant.now()),
          None,
          Some("Defined"),
          Some("text"),
          Some(true),
          None,
          Some("PropertyGroup1")
        ),
        FilepropertyRow(
          "TopLevelProperty2",
          None,
          Some("Top Level Property 2"),
          Timestamp.from(Instant.now()),
          None,
          Some("Defined"),
          Some("text"),
          Some(true),
          None,
          Some("PropertyGroup2")
        )
      )
    )
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

    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val fileMetadataDeleteCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val expectedPropertyNamesToDelete: Seq[String] = Seq("ClosureType", "TitleClosed", "Property1", "TopLevelProperty1")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))
    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), fileMetadataDeleteCaptor.capture())).thenReturn(Future(2))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service
      .deleteFileMetadata(
        DeleteFileMetadataInput(
          Seq(folderId),
          Seq(
            FileMetadataToDelete(ClosureType, Some("Closed")),
            FileMetadataToDelete("TopLevelProperty1", None)
          )
        ),
        userId
      )
      .futureValue

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete)
    val addFileMetadata = addFileMetadataCaptor.getValue
    val fileMetadataDelete = fileMetadataDeleteCaptor.getValue

    addFileMetadata.size should equal(4)
    fileMetadataDelete.size should equal(4)

    fileMetadataDelete should equal(Set(ClosureType, TitleClosed, "Property1", "TopLevelProperty1"))
  }

  "deleteFileMetadata" should "delete and reset fileMetadata properties with a default value for the selected files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow(
          "ClosureStartDate",
          None,
          Some("Closure Start Date"),
          Timestamp.from(Instant.now()),
          None,
          Some("Defined"),
          Some("text"),
          Some(true),
          None,
          Some("Closure")
        ),
        FilepropertyRow("TitleClosed", None, Some("Title Closed"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("Property1", None, Some("Property 1"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Property1Group"))
      )
    )
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

    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val expectedPropertyNamesToDelete = Set("Property1", "ClosureStartDate", "ClosurePeriod", "TitleClosed", "ClosureType")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), ArgumentMatchers.eq(expectedPropertyNamesToDelete))).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId), Seq(FileMetadataToDelete(ClosureType, Some("Closed")))), userId).futureValue

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete.toSeq)
    val addFileMetadata = addFileMetadataCaptor.getValue
    addFileMetadata.size should equal(4)

    val expectedPropertyNames = List(TitleClosed, ClosureType)
    val expectedPropertyValues = List("false", "Open")
    addFileMetadata.foreach { metadata =>
      expectedPropertyNames.contains(metadata.propertyname) shouldBe true
      expectedPropertyValues.contains(metadata.value) shouldBe true
      metadata.datetime != null shouldBe true
      metadata.userid should equal(userId)
    }
  }

  "deleteFileMetadata" should "delete and reset all the dependencies of the property passed in, even if no value was given" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow(
          "ClosureStartDate",
          None,
          Some("Closure Start Date"),
          Timestamp.from(Instant.now()),
          None,
          Some("Defined"),
          Some("text"),
          Some(true),
          None,
          Some("Closure")
        ),
        FilepropertyRow("TitleClosed", None, Some("Title Closed"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("Property1", None, Some("Property 1"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Property1Group"))
      )
    )
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

    val addFileMetadataCaptor: ArgumentCaptor[Seq[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[Seq[FilemetadataRow]])
    val fileMetadataDeleteCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val expectedPropertyNamesToDelete = List("Property1", "ClosureStartDate", "ClosurePeriod", "TitleClosed", "ClosureType")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), fileMetadataDeleteCaptor.capture())).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())
    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId), Seq(FileMetadataToDelete(ClosureType, None))), userId).futureValue

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete)
    val addfileMetadata: Seq[FilemetadataRow] = addFileMetadataCaptor.getValue
    val fileMetadataDelete = fileMetadataDeleteCaptor.getValue

    addfileMetadata.size should equal(4)
    val expectedPropertyNames = List(TitleClosed, ClosureType)
    val expectedPropertyValues = List("false", "Open")

    addfileMetadata.foreach { metadata =>
      expectedPropertyNames.contains(metadata.propertyname) shouldBe true
      expectedPropertyValues.contains(metadata.value) shouldBe true
      metadata.datetime != null shouldBe true
      metadata.userid should equal(userId)
    }

    fileMetadataDelete.size should equal(5)
    fileMetadataDelete should equal(Set("Property1", "ClosureStartDate", "ClosurePeriod", TitleClosed, ClosureType))
  }

  "deleteFileMetadata" should "throw an exception if value passed in is missing in the db" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val userId = UUID.randomUUID()
    val folderId = UUID.fromString("e3fce276-2615-4a3a-aa4e-67f9a65798cf")
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val existingFileRows: Seq[FileRow] = generateFileRows(Seq(folderId), Seq(folderId, fileInFolderId1, fileInFolderId2), userId)
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosureType", "Closed", None, Some(1), None, None),
        FilepropertyvaluesRow("TitlePublic", "ABC", None, Some(1), None, None)
      )
    )

    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "TitleClosed", None),
        FilepropertydependenciesRow(2, "Property1", None)
      )
    )
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepositoryMock.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)
    when(fileRepositoryMock.getAllDescendants(ArgumentMatchers.eq(Seq(folderId)))).thenReturn(Future(existingFileRows))

    verify(fileMetadataRepositoryMock, times(0)).addFileMetadata(any[Seq[FilemetadataRow]])
    verify(fileMetadataRepositoryMock, times(0)).deleteFileMetadata(any[Set[UUID]], any[Set[String]])
    verify(customMetadataPropertiesRepositoryMock, times(0)).getCustomMetadataDependencies

    val service = new FileMetadataService(fileMetadataRepositoryMock, fileRepositoryMock, customMetadataPropertiesRepositoryMock, FixedTimeSource, new FixedUUIDSource())

    val thrownException = intercept[Exception] {
      service.deleteFileMetadata(DeleteFileMetadataInput(Seq(folderId), Seq(FileMetadataToDelete(ClosureType, Some("NonExistentValue")))), UUID.randomUUID()).futureValue
    }

    thrownException.getMessage should include("Can't find metadata property 'ClosureType' with value 'Some(NonExistentValue)' in the db.")
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
    val childFileId1: UUID = UUID.randomUUID()
    val childFileId2: UUID = UUID.randomUUID()

    val folderAndChildrenIds: Seq[UUID] = Seq(folderId, childFileId1, childFileId2)
    val inputFileIds: Seq[UUID] = Seq(folderId, fileId1)

    val newMetadataProperties: Seq[UpdateFileMetadataInput] = Seq(
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "propertyName1", "newValue1"),
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "propertyName2", "newValue2"),
      UpdateFileMetadataInput(filePropertyIsMultiValue = true, "propertyName3", "newValue3"),
      UpdateFileMetadataInput(filePropertyIsMultiValue = true, "propertyName3", "newValue4")
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
