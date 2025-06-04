package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{Boolean, CustomMetadataField, CustomMetadataValues, Defined, Supplied, System, Text}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{SHA256ServerSideChecksum, _}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType.{directoryTypeIdentifier, fileTypeIdentifier}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ClosureMetadata, DescriptiveMetadata}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addFileMetadata" should "call the metadata repository with the correct row arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val addChecksumCaptor: ArgumentCaptor[List[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[List[AddFileMetadataInput]])
    val getFileMetadataFileCaptor: ArgumentCaptor[List[UUID]] = ArgumentCaptor.forClass(classOf[List[UUID]])
    val getFileMetadataPropertyNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val mockFileMetadataResponse =
      Seq(FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ClientSideChecksum))

    when(metadataRepositoryMock.addFileMetadata(addChecksumCaptor.capture()))
      .thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(getFileMetadataFileCaptor.capture(), getFileMetadataPropertyNameCaptor.capture()))
      .thenReturn(Future(mockFileMetadataResponse))
    when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(List()))

    val service = new FileMetadataService(metadataRepositoryMock, consignmentStatusServiceMock, customMetadataServiceMock, validateFileMetadataServiceMock, fileStatusServiceMock)
    service.addFileMetadata(createInput(SHA256ServerSideChecksum, fixedFileUuid, "value"), fixedUserId).futureValue

    verify(validateFileMetadataServiceMock, times(0)).validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])

    val row = addChecksumCaptor.getValue.head
    row.filePropertyName should equal(SHA256ServerSideChecksum)
    row.fileId should equal(fixedFileUuid)
    row.userId should equal(fixedUserId)
  }

  def createInput(propertyName: String, fileId: UUID, value: String): AddFileMetadataWithFileIdInput = {
    AddFileMetadataWithFileIdInput(AddFileMetadataWithFileIdInputValues(propertyName, fileId, value) :: Nil)
  }

  "addFileMetadata" should "return the correct data for a single update" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUserId = UUID.fromString("61b49923-daf7-4140-98f1-58ba6cbed61f")
    val value = "value"
    val metadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val propertyName = SHA256ServerSideChecksum
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))

    when(metadataRepositoryMock.addFileMetadata(any[List[AddFileMetadataInput]])).thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid :: Nil, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, consignmentStatusServiceMock, customMetadataServiceMock, validateFileMetadataServiceMock, fileStatusServiceMock)
    val result: FileMetadataWithFileId =
      service.addFileMetadata(createInput(propertyName, fixedFileUuid, "value"), fixedUserId).futureValue.head

    result.fileId should equal(fixedFileUuid)
    result.filePropertyName should equal(propertyName)
    result.value should equal(value)
  }

  "updateBulkFileMetadata" should "delete existing metadata rows and add new metadata rows based on the input" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()
    val fileIds = Seq(testSetUp.fileId1, testSetUp.childFileId1, testSetUp.childFileId2)
    val existingFileRows: Seq[FileRow] = generateFileRows(fileIds, Seq(), testSetUp.userId)

    testSetUp.stubRepoResponses(existingFileRows)
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, fileIds, testSetUp.newMetadataProperties)
    val expectedPropertyNames = input.metadataProperties.map(_.filePropertyName).toSet

    service.updateBulkFileMetadata(input, testSetUp.userId).futureValue
    verify(testSetUp.validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(fileIds.toSet, expectedPropertyNames)

    val deleteFileMetadataIdsArg: Set[UUID] = testSetUp.deleteFileMetadataIdsArgCaptor.getValue
    val deleteFileMetadataPropertiesArg: Set[String] = testSetUp.deletePropertyNamesCaptor.getValue
    val addFileMetadataArgument: Seq[AddFileMetadataInput] = testSetUp.addFileMetadataCaptor.getValue

    val expectedUpdatedIds: Set[UUID] = Set(testSetUp.fileId1, testSetUp.childFileId1, testSetUp.childFileId2)
    val expectedUpdatedPropertyNames: Set[String] = Set("propertyName1", "propertyName2", "propertyName3")
    val expectedUpdatedPropertyValues: Set[String] = Set("newValue1", "newValue2", "newValue3", "newValue4")

    deleteFileMetadataIdsArg should equal(expectedUpdatedIds)
    deleteFileMetadataPropertiesArg should equal(expectedUpdatedPropertyNames)

    addFileMetadataArgument.size should equal(12)
    val addedFileIds = addFileMetadataArgument.map(_.fileId).toSet
    addedFileIds.size should equal(deleteFileMetadataIdsArg.size)
    addedFileIds.subsetOf(expectedUpdatedIds) should equal(true)

    val addedPropertyValues = addFileMetadataArgument.map(_.value).toSet
    addedPropertyValues.size should equal(4)
    addedPropertyValues.subsetOf(expectedUpdatedPropertyValues) should equal(true)

    val addedProperties = addFileMetadataArgument.map(_.filePropertyName).toSet
    addedProperties.size should equal(3)
    addedProperties.subsetOf(expectedUpdatedPropertyNames) should equal(true)
  }

  "updateBulkFileMetadata" should "not update properties, and throw an error if input contains an empty property value" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()
    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.inputFileIds, testSetUp.folderAndChildrenIds, testSetUp.userId)

    testSetUp.stubRepoResponses(existingFileRows)
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val emptyMetadataProperties: Seq[UpdateFileMetadataInput] = Seq(
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "Property1", ""),
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "Property2", "some value"),
      UpdateFileMetadataInput(filePropertyIsMultiValue = false, "Property3", "")
    )

    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, emptyMetadataProperties)

    val thrownException = intercept[Exception] {
      service.updateBulkFileMetadata(input, testSetUp.userId).futureValue
    }

    verify(testSetUp.fileRepositoryMock, times(0)).getAllDescendants(any[Seq[UUID]])
    verify(testSetUp.metadataRepositoryMock, times(0)).deleteFileMetadata(any[Set[UUID]], any[Set[String]])
    verify(testSetUp.metadataRepositoryMock, times(0)).addFileMetadata(any[Seq[AddFileMetadataInput]])
    verify(testSetUp.validateFileMetadataServiceMock, times(0)).validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])

    thrownException.getMessage should include("Cannot update properties with empty value: Property1, Property3")
  }

  "updateBulkFileMetadata" should "create the metadata consignment statuses" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.inputFileIds, testSetUp.folderAndChildrenIds, testSetUp.userId)
    val fileStatusRows = generateFileStatusRows(testSetUp.inputFileIds)
    testSetUp.stubRepoResponses(existingFileRows)
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val statusTypeCaptor: ArgumentCaptor[List[String]] = ArgumentCaptor.forClass(classOf[List[String]])
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(consignmentIdCaptor.capture(), statusTypeCaptor.capture()))
      .thenReturn(Future.successful(1 :: Nil))
    when(testSetUp.validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]]))
      .thenReturn(Future.successful(fileStatusRows.toList))
    val input = UpdateBulkFileMetadataInput(testSetUp.consignmentId, testSetUp.inputFileIds, testSetUp.newMetadataProperties)

    val service =
      new FileMetadataService(
        testSetUp.metadataRepositoryMock,
        consignmentStatusServiceMock,
        testSetUp.customMetadataServiceMock,
        testSetUp.validateFileMetadataServiceMock,
        testSetUp.fileStatusServiceMock
      )
    service.updateBulkFileMetadata(input, testSetUp.userId).futureValue

    verify(consignmentStatusServiceMock, times(1))
      .updateMetadataConsignmentStatus(any[UUID], any[List[String]])
    consignmentIdCaptor.getValue should equal(testSetUp.consignmentId)
    statusTypeCaptor.getValue.sorted should equal(List(ClosureMetadata, DescriptiveMetadata))
  }

  "addOrUpdateBulkFileMetadata" should "delete existing metadata rows and add new metadata rows based on the input" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val addOrUpdateBulkFileMetadata = testSetUp.inputFileIds.map(fileId =>
      AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", "newValue2")))
    )

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)
    val expectedPropertyNames = Set("propertyName1", "propertyName2")

    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue
    verify(testSetUp.validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(testSetUp.inputFileIds.toSet, expectedPropertyNames)
    verify(testSetUp.metadataRepositoryMock, times(3)).deleteFileMetadata(any[UUID], any[Set[String]])

    val deleteFileMetadataIdsArg: UUID = testSetUp.deleteFileMetadataIdsArgCaptor.getValue
    val deleteFileMetadataPropertiesArg: Set[String] = testSetUp.deletePropertyNamesCaptor.getValue
    val addFileMetadataArgument: Seq[AddFileMetadataInput] = testSetUp.addFileMetadataCaptor.getValue

    val expectedUpdatedPropertyNames: Set[String] = Set("propertyName1", "propertyName2")
    val expectedUpdatedPropertyValues: Set[String] = Set("newValue1", "newValue2")

    deleteFileMetadataIdsArg should equal(testSetUp.fileId3)
    deleteFileMetadataPropertiesArg should equal(expectedUpdatedPropertyNames)

    addFileMetadataArgument.size should equal(6)
    val addedFileIds = addFileMetadataArgument.map(_.fileId).toSet
    addedFileIds should equal(testSetUp.inputFileIds.toSet)

    val addedPropertyValues = addFileMetadataArgument.map(_.value).toSet
    addedPropertyValues.size should equal(2)
    addedPropertyValues.subsetOf(expectedUpdatedPropertyValues) should equal(true)

    val addedProperties = addFileMetadataArgument.map(_.filePropertyName).toSet
    addedProperties.size should equal(2)
    addedProperties.subsetOf(expectedUpdatedPropertyNames) should equal(true)
  }

  "addOrUpdateBulkFileMetadata" should "not update properties, and throw an error if input contains a protected property" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val addOrUpdateBulkFileMetadata = testSetUp.inputFileIds.map(fileId =>
      AddOrUpdateFileMetadata(
        fileId,
        List(
          AddOrUpdateMetadata("propertyName1", "newValue1"),
          AddOrUpdateMetadata("SHA256ServerSideChecksum", "newValue2"),
          AddOrUpdateMetadata("propertyName3", "")
        )
      )
    )

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)

    val thrownException = intercept[Exception] {
      service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue
    }

    verify(testSetUp.metadataRepositoryMock, times(0)).deleteFileMetadata(any[UUID], any[Set[String]])
    verify(testSetUp.metadataRepositoryMock, times(0)).addFileMetadata(any[Seq[AddFileMetadataInput]])
    verify(testSetUp.validateFileMetadataServiceMock, times(0)).validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])

    thrownException.getMessage should include("Protected metadata property found: SHA256ServerSideChecksum")
  }

  "addOrUpdateBulkFileMetadata" should "delete properties with empty values but not add empty properties" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val addOrUpdateBulkFileMetadata =
      testSetUp.inputFileIds.map(fileId => AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", ""))))

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)
    val expectedPropertyNames = Set("propertyName1", "propertyName2")

    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue
    verify(testSetUp.validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(testSetUp.inputFileIds.toSet, expectedPropertyNames)
    verify(testSetUp.metadataRepositoryMock, times(3)).deleteFileMetadata(any[UUID], any[Set[String]])

    val deleteFileMetadataPropertiesArg: Set[String] = testSetUp.deletePropertyNamesCaptor.getValue
    val addFileMetadataArgument: Seq[AddFileMetadataInput] = testSetUp.addFileMetadataCaptor.getValue

    val expectedUpdatedPropertyNames: Set[String] = Set("propertyName1", "propertyName2")

    deleteFileMetadataPropertiesArg.size should equal(2)
    deleteFileMetadataPropertiesArg should equal(expectedUpdatedPropertyNames)

    addFileMetadataArgument.size should equal(3)
    val addedFileIds = addFileMetadataArgument.map(_.fileId).toSet
    addedFileIds should equal(testSetUp.inputFileIds.toSet)

    val addedPropertyValues = addFileMetadataArgument.map(_.value).toSet
    addedPropertyValues.size should equal(1)

    val addedProperties = addFileMetadataArgument.map(_.filePropertyName).toSet
    addedProperties.size should equal(1)
  }

  "addOrUpdateBulkFileMetadata" should "add or updated metadata and metadata statuses when skipValidation is 'false'" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val addOrUpdateBulkFileMetadata = testSetUp.inputFileIds.map(fileId =>
      AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", "newValue2")))
    )

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)
    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue

    verify(testSetUp.consignmentStatusServiceMock, times(1))
      .updateMetadataConsignmentStatus(any[UUID], any[List[String]])
    testSetUp.consignmentIdCaptor.getValue should equal(testSetUp.consignmentId)
    testSetUp.statusTypeCaptor.getValue.sorted should equal(List(ClosureMetadata, DescriptiveMetadata))
  }

  "addOrUpdateBulkFileMetadata" should "add or updated metadata and metadata statuses without validating the data when skipValidation is 'true'" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      testSetUp.consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )

    val addOrUpdateBulkFileMetadata = testSetUp.inputFileIds.map(fileId =>
      AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", "newValue2")))
    )

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata, skipValidation = true)
    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue

    verify(testSetUp.validateFileMetadataServiceMock, times(0)).validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])
    verify(testSetUp.fileStatusServiceMock, times(1)).addAdditionalMetadataStatuses(any[List[AddOrUpdateFileMetadata]])
    verify(testSetUp.consignmentStatusServiceMock, times(1)).updateMetadataConsignmentStatus(any[UUID], any[List[String]])

    testSetUp.consignmentIdCaptor.getValue should equal(testSetUp.consignmentId)
    testSetUp.statusTypeCaptor.getValue.sorted should equal(List(ClosureMetadata, DescriptiveMetadata))
  }

  "getFileMetadata" should "call the repository with the correct arguments" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val consignmentIdCaptor: ArgumentCaptor[Option[UUID]] = ArgumentCaptor.forClass(classOf[Option[UUID]]) // Corrected type for the captor
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()
    val mockResponse = Future(Seq())
    when(fileMetadataRepositoryMock.getFileMetadata(any[Option[UUID]], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(mockResponse)

    val service =
      new FileMetadataService(fileMetadataRepositoryMock, consignmentStatusServiceMock, customMetadataServiceMock, validateFileMetadataServiceMock, fileStatusServiceMock)

    service.getFileMetadata(Some(consignmentId)).futureValue

    // Verify mock interaction and capture arguments before asserting them
    verify(fileMetadataRepositoryMock).getFileMetadata(
      consignmentIdCaptor.capture(),
      selectedFileIdsCaptor.capture(),
      any[Option[Set[String]]]
    )

    consignmentIdCaptor.getValue should equal(Some(consignmentId))
    selectedFileIdsCaptor.getValue should equal(None)
  }

  "getFileMetadata" should "return multiple map entries for multiple files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
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

    when(fileMetadataRepositoryMock.getFileMetadata(Some(any[UUID]), any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(mockResponse)

    val service =
      new FileMetadataService(fileMetadataRepositoryMock, consignmentStatusServiceMock, customMetadataServiceMock, validateFileMetadataServiceMock, fileStatusServiceMock)
    val response = service.getFileMetadata(Some(consignmentId)).futureValue

    response.size should equal(2)
    response.contains(fileIdOne) should equal(true)
    response.contains(fileIdTwo) should equal(true)
    response(fileIdOne).clientSideFileSize.get should equal(1)
    response(fileIdTwo).foiExemptionCode.get should equal("valueTwo")
    response(fileIdTwo).closureStartDate.get should equal(closureStartDate.toLocalDateTime)
    response(fileIdTwo).closurePeriod.get should equal("1")
    response(fileIdTwo).titleClosed.get should equal(true)
    response(fileIdTwo).descriptionClosed.get should equal(true)
    response(fileIdTwo).foiExemptionAsserted.get should equal(foiExemptionAsserted.toLocalDateTime)
  }

  "getSumOfFileSizes" should "return the sum of the file sizes" in {
    val fileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusServiceMock = mock[FileStatusService]
    val consignmentId = UUID.randomUUID()
    when(fileMetadataRepository.getSumOfFileSizes(any[UUID])).thenReturn(Future(1L))

    val service = new FileMetadataService(
      fileMetadataRepository,
      mock[ConsignmentStatusService],
      mock[CustomMetadataPropertiesService],
      mock[ValidateFileMetadataService],
      fileStatusServiceMock
    )
    val result = service.getSumOfFileSizes(consignmentId).futureValue

    result should equal(1)
    verify(fileMetadataRepository, times(1)).getSumOfFileSizes(consignmentId)
  }

  "deleteFileMetadata" should "throw an exception if property name does not exist" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val fileOneId = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileTwoId = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val customMetadataSetUp = new CustomMetadataTestSetUp()
    customMetadataSetUp.stubResponse()

    val service =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        consignmentStatusServiceMock,
        customMetadataSetUp.customMetadataServiceMock,
        validateFileMetadataServiceMock,
        fileStatusServiceMock
      )

    val thrownException = intercept[Exception] {
      service.deleteFileMetadata(DeleteFileMetadataInput(Seq(fileOneId, fileTwoId), Seq("Non-ExistentProperty"), UUID.randomUUID()), UUID.randomUUID()).futureValue
    }

    verify(fileMetadataRepositoryMock, times(0)).addFileMetadata(any[Seq[AddFileMetadataInput]])
    verify(fileMetadataRepositoryMock, times(0)).deleteFileMetadata(any[Set[UUID]], any[Set[String]])
    verify(customMetadataSetUp.customMetadataServiceMock, times(1)).getCustomMetadata
    verify(validateFileMetadataServiceMock, times(0)).validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])
    verify(consignmentStatusServiceMock, times(0)).updateMetadataConsignmentStatus(any[UUID], any[List[String]])

    thrownException.getMessage should include("Can't find metadata property 'Non-ExistentProperty' in the db.")
  }

  "deleteFileMetadata" should "delete and/or reset multiple properties including any dependencies" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val fileMetadataDeleteCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val expectedPropertyNamesToDelete: Seq[String] = Seq("TopLevelProperty1", "ClosureStartDate", "ClosurePeriod", "TitleClosed", "ClosureType")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    val customMetadataTestSetUp = new CustomMetadataTestSetUp()
    customMetadataTestSetUp.stubResponse()

    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))
    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), fileMetadataDeleteCaptor.capture())).thenReturn(Future(2))
    when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(Nil))
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(any[UUID], any[List[String]])).thenReturn(Future.successful(1 :: Nil))

    val service =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        consignmentStatusServiceMock,
        customMetadataTestSetUp.customMetadataServiceMock,
        validateFileMetadataServiceMock,
        fileStatusServiceMock
      )
    val response = service
      .deleteFileMetadata(
        DeleteFileMetadataInput(
          Seq(fileInFolderId1, fileInFolderId2),
          Seq(
            ClosureType,
            "TopLevelProperty1"
          ),
          consignmentId
        ),
        userId
      )
      .futureValue

    verify(validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(fileIds.toSet, expectedPropertyNamesToDelete.toSet)
    verify(consignmentStatusServiceMock, times(1)).updateMetadataConsignmentStatus(consignmentId, List(DescriptiveMetadata, ClosureMetadata))

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete)
    val addFileMetadata = addFileMetadataCaptor.getValue
    val fileMetadataDelete = fileMetadataDeleteCaptor.getValue

    addFileMetadata.size should equal(2)
    fileMetadataDelete.size should equal(5)

    fileMetadataDelete should equal(Set("TopLevelProperty1", "ClosureStartDate", "ClosurePeriod", "TitleClosed", ClosureType))
  }

  "deleteFileMetadata" should "delete and reset fileMetadata properties with a default value for the selected files" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val expectedPropertyNamesToDelete = Set("ClosurePeriod", "ClosureStartDate", "TitleClosed", "ClosureType")

    val customMetadataTestSetUp = new CustomMetadataTestSetUp()
    customMetadataTestSetUp.stubResponse()
    val fileIds = Seq(fileInFolderId1, fileInFolderId2)

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), ArgumentMatchers.eq(expectedPropertyNamesToDelete))).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))
    when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(Nil))
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(any[UUID], any[List[String]])).thenReturn(Future.successful(1 :: Nil))

    val service =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        consignmentStatusServiceMock,
        customMetadataTestSetUp.customMetadataServiceMock,
        validateFileMetadataServiceMock,
        fileStatusServiceMock
      )

    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(fileInFolderId1, fileInFolderId2), Seq(ClosureType), consignmentId), userId).futureValue
    verify(validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(fileIds.toSet, expectedPropertyNamesToDelete)
    verify(consignmentStatusServiceMock, times(1)).updateMetadataConsignmentStatus(consignmentId, List(DescriptiveMetadata, ClosureMetadata))

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete.toSeq)
    val addFileMetadata = addFileMetadataCaptor.getValue
    addFileMetadata.size should equal(2)

    val expectedPropertyNames = List(TitleClosed, ClosureType)
    val expectedPropertyValues = List("false", "Open")
    addFileMetadata.foreach { metadata =>
      expectedPropertyNames.contains(metadata.filePropertyName) shouldBe true
      expectedPropertyValues.contains(metadata.value) shouldBe true
      metadata.userId should equal(userId)
    }
  }

  "deleteFileMetadata" should "handle deleting 'description' property correctly" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val expectedPropertyNamesToDelete = Set("description", "DescriptionAlternate", "DescriptionClosed")

    val customMetadataTestSetUp = new CustomMetadataTestSetUp()
    customMetadataTestSetUp.stubResponse()

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), ArgumentMatchers.eq(expectedPropertyNamesToDelete))).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))
    when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(Nil))
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(any[UUID], any[List[String]])).thenReturn(Future.successful(1 :: Nil))

    val service =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        consignmentStatusServiceMock,
        customMetadataTestSetUp.customMetadataServiceMock,
        validateFileMetadataServiceMock,
        fileStatusServiceMock
      )

    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(fileInFolderId1, fileInFolderId2), Seq("description"), consignmentId), userId).futureValue
    verify(validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(fileIds.toSet, expectedPropertyNamesToDelete)
    verify(consignmentStatusServiceMock, times(1)).updateMetadataConsignmentStatus(consignmentId, List(DescriptiveMetadata, ClosureMetadata))

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete.toSeq)
    val addFileMetadata = addFileMetadataCaptor.getValue
    addFileMetadata.size should equal(2)

    val expectedPropertyNames = List(DescriptionClosed)
    val expectedPropertyValues = List("false")
    addFileMetadata.foreach { metadata =>
      expectedPropertyNames.contains(metadata.filePropertyName) shouldBe true
      expectedPropertyValues.contains(metadata.value) shouldBe true
      metadata.userId should equal(userId)
    }
  }

  "deleteFileMetadata" should "delete and reset all the dependencies of the property passed in, even if no value was given" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val validateFileMetadataServiceMock = mock[ValidateFileMetadataService]
    val fileStatusServiceMock = mock[FileStatusService]
    val userId = UUID.randomUUID()
    val consignmentId = UUID.randomUUID()
    val fileInFolderId1 = UUID.fromString("104dde28-21cc-43f6-aa47-d17f120497f5")
    val fileInFolderId2 = UUID.fromString("81643ecc-e618-43bb-829e-f7266565d0b5")

    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val fileMetadataDeleteCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val expectedPropertyNamesToDelete = List("ClosurePeriod", "ClosureStartDate", "TitleClosed", "ClosureType")

    val fileIds = Seq(fileInFolderId1, fileInFolderId2)
    val customMetadataTestSetUp = new CustomMetadataTestSetUp()
    customMetadataTestSetUp.stubResponse()

    when(fileMetadataRepositoryMock.deleteFileMetadata(ArgumentMatchers.eq(fileIds.toSet), fileMetadataDeleteCaptor.capture())).thenReturn(Future(2))
    when(fileMetadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture())).thenReturn(Future(Nil))
    when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(Nil))
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(any[UUID], any[List[String]])).thenReturn(Future.successful(1 :: Nil))

    val service =
      new FileMetadataService(
        fileMetadataRepositoryMock,
        consignmentStatusServiceMock,
        customMetadataTestSetUp.customMetadataServiceMock,
        validateFileMetadataServiceMock,
        fileStatusServiceMock
      )
    val response = service.deleteFileMetadata(DeleteFileMetadataInput(Seq(fileInFolderId1, fileInFolderId2), Seq(ClosureType), consignmentId), userId).futureValue
    verify(validateFileMetadataServiceMock, times(1)).validateAdditionalMetadata(fileIds.toSet, expectedPropertyNamesToDelete.toSet)
    verify(consignmentStatusServiceMock, times(1)).updateMetadataConsignmentStatus(consignmentId, List(DescriptiveMetadata, ClosureMetadata))

    response.fileIds should equal(fileIds)
    response.filePropertyNames should equal(expectedPropertyNamesToDelete)
    val addfileMetadata: Seq[AddFileMetadataInput] = addFileMetadataCaptor.getValue
    val fileMetadataDelete = fileMetadataDeleteCaptor.getValue

    addfileMetadata.size should equal(2)
    val expectedPropertyNames = List(TitleClosed, ClosureType)
    val expectedPropertyValues = List("false", "Open")

    addfileMetadata.foreach { metadata =>
      expectedPropertyNames.contains(metadata.filePropertyName) shouldBe true
      expectedPropertyValues.contains(metadata.value) shouldBe true
      metadata.userId should equal(userId)
    }

    fileMetadataDelete.size should equal(4)
    fileMetadataDelete should equal(Set("ClosurePeriod", "ClosureStartDate", TitleClosed, ClosureType))
  }

  "deleteFileMetadata" should "create the metadata consignment statuses" in {
    val testSetUp = new UpdateBulkMetadataTestSetUp()
    val existingFileRows: Seq[FileRow] = generateFileRows(testSetUp.inputFileIds, testSetUp.folderAndChildrenIds, testSetUp.userId)
    val fileStatusRows = generateFileStatusRows(testSetUp.inputFileIds)
    testSetUp.stubRepoResponses(existingFileRows)
    val consignmentStatusServiceMock = mock[ConsignmentStatusService]
    val customMetadataSetUp = new CustomMetadataTestSetUp()
    customMetadataSetUp.stubResponse()

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val statusTypeCaptor: ArgumentCaptor[List[String]] = ArgumentCaptor.forClass(classOf[List[String]])
    when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(consignmentIdCaptor.capture(), statusTypeCaptor.capture()))
      .thenReturn(Future.successful(1 :: Nil))
    when(testSetUp.validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]]))
      .thenReturn(Future.successful(fileStatusRows.toList))
    val input = DeleteFileMetadataInput(Seq(testSetUp.childFileId1, testSetUp.fileId1, testSetUp.childFileId2), Seq(ClosureType), testSetUp.consignmentId)

    val service = new FileMetadataService(
      testSetUp.metadataRepositoryMock,
      consignmentStatusServiceMock,
      customMetadataSetUp.customMetadataServiceMock,
      testSetUp.validateFileMetadataServiceMock,
      testSetUp.fileStatusServiceMock
    )
    service.deleteFileMetadata(input, testSetUp.userId).futureValue

    verify(consignmentStatusServiceMock, times(1))
      .updateMetadataConsignmentStatus(any[UUID], any[List[String]])
    consignmentIdCaptor.getValue should equal(testSetUp.consignmentId)
    statusTypeCaptor.getValue.sorted should equal(List(ClosureMetadata, DescriptiveMetadata))
  }

  "file metadata property names" should "have the correct values" in {
    FileMetadataFields.SHA256ServerSideChecksum should equal("SHA256ServerSideChecksum")
  }

  "The FileMetadataService property names" should "have the correct values" in {
    FileMetadataService.SHA256ClientSideChecksum shouldEqual "SHA256ClientSideChecksum"
    FileMetadataService.ClientSideOriginalFilepath shouldEqual "ClientSideOriginalFilepath"
    FileMetadataService.OriginalFilepath shouldEqual "OriginalFilepath"
    FileMetadataService.ClientSideFileLastModifiedDate shouldEqual "ClientSideFileLastModifiedDate"
    FileMetadataService.ClientSideFileSize shouldEqual "ClientSideFileSize"
    FileMetadataService.ClosurePeriod shouldEqual "ClosurePeriod"
    FileMetadataService.ClosureStartDate shouldEqual "ClosureStartDate"
    FileMetadataService.Filename shouldEqual "Filename"
    FileMetadataService.FileType shouldEqual "FileType"
    FileMetadataService.FileReference shouldEqual "FileReference"
    FileMetadataService.ParentReference shouldEqual "ParentReference"
    FileMetadataService.FoiExemptionAsserted shouldEqual "FoiExemptionAsserted"
    FileMetadataService.TitleClosed shouldEqual "TitleClosed"
    FileMetadataService.DescriptionClosed shouldEqual "DescriptionClosed"
    FileMetadataService.ClosureType shouldEqual "ClosureType"
    FileMetadataService.Description shouldEqual "description"
    FileMetadataService.DescriptionAlternate shouldEqual "DescriptionAlternate"
    FileMetadataService.RightsCopyright shouldEqual "RightsCopyright"
    FileMetadataService.LegalStatus shouldEqual "LegalStatus"
    FileMetadataService.HeldBy shouldEqual "HeldBy"
    FileMetadataService.Language shouldEqual "Language"
    FileMetadataService.FoiExemptionCode shouldEqual "FoiExemptionCode"
    FileMetadataService.FileUUID shouldEqual "UUID"
  }

  private def generateFileStatusRows(fileIds: Seq[UUID]) = {
    fileIds.map(id => FilestatusRow(UUID.randomUUID(), id, "statusType", "value", Timestamp.from(FixedTimeSource.now)))
  }

  private def generateFileRows(fileUuids: Seq[UUID], filesInFolderFixedFileUuids: Seq[UUID], fixedUserId: UUID, consignmentId: UUID = UUID.randomUUID()): Seq[FileRow] = {
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

  private class UpdateBulkMetadataTestSetUp() {
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
    val customMetadataServiceMock: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val validateFileMetadataServiceMock: ValidateFileMetadataService = mock[ValidateFileMetadataService]
    val consignmentStatusServiceMock: ConsignmentStatusService = mock[ConsignmentStatusService]
    val fileStatusServiceMock: FileStatusService = mock[FileStatusService]

    val getAllDescendentIdsCaptor: ArgumentCaptor[Seq[UUID]] = ArgumentCaptor.forClass(classOf[Seq[UUID]])
    val deletePropertyNamesCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
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
      when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(List()))
      when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(any[UUID], any[List[String]]))
        .thenReturn(Future.successful(1 :: Nil))
    }
  }

  private class AddOrUpdateBulkMetadataTestSetUp() {
    val userId: UUID = UUID.randomUUID()
    val consignmentId: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()
    val fileId3: UUID = UUID.randomUUID()

    val inputFileIds: Seq[UUID] = Seq(fileId1, fileId2, fileId3)

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    val metadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
    val validateFileMetadataServiceMock: ValidateFileMetadataService = mock[ValidateFileMetadataService]
    val consignmentStatusServiceMock: ConsignmentStatusService = mock[ConsignmentStatusService]
    val fileStatusServiceMock: FileStatusService = mock[FileStatusService]

    val deletePropertyNamesCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val deleteFileMetadataIdsArgCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val statusTypeCaptor: ArgumentCaptor[List[String]] = ArgumentCaptor.forClass(classOf[List[String]])
    def stubRepoResponses(
        deleteFileMetadataResponse: Int = 0,
        addFileMetadataResponse: Seq[FilemetadataRow] = Seq()
    ): Unit = {

      when(metadataRepositoryMock.deleteFileMetadata(deleteFileMetadataIdsArgCaptor.capture(), deletePropertyNamesCaptor.capture()))
        .thenReturn(Future(deleteFileMetadataResponse))
      when(metadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture()))
        .thenReturn(Future(addFileMetadataResponse))
      when(validateFileMetadataServiceMock.validateAdditionalMetadata(any[Set[UUID]], any[Set[String]])).thenReturn(Future(List()))
      when(fileStatusServiceMock.addAdditionalMetadataStatuses(any[List[AddOrUpdateFileMetadata]])).thenReturn(Future(List()))
      when(consignmentStatusServiceMock.updateMetadataConsignmentStatus(consignmentIdCaptor.capture(), statusTypeCaptor.capture()))
        .thenReturn(Future.successful(1 :: Nil))
    }
  }

  private class CustomMetadataTestSetUp {
    val customMetadataServiceMock: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]

    private val closurePropertyGroup: Option[String] = Some("Closure")

    private val alternativeDescriptionField: CustomMetadataField =
      CustomMetadataField(
        DescriptionAlternate,
        Some(DescriptionAlternate),
        None,
        Supplied,
        closurePropertyGroup,
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val closurePeriodField: CustomMetadataField =
      CustomMetadataField(
        "ClosurePeriod",
        Some("Closure Period"),
        None,
        Defined,
        closurePropertyGroup,
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val closureStartDateField: CustomMetadataField =
      CustomMetadataField(
        "ClosureStartDate",
        Some("Closure Start date"),
        None,
        Defined,
        closurePropertyGroup,
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val descriptionField: CustomMetadataField =
      CustomMetadataField(
        Description,
        Some(Description),
        None,
        Defined,
        Some("OptionalMetadata"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val titleClosedField: CustomMetadataField =
      CustomMetadataField(
        "TitleClosed",
        Some("Title Closed"),
        None,
        Defined,
        closurePropertyGroup,
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val topLevelDependencyField: CustomMetadataField =
      CustomMetadataField(
        "TopLevelProperty1",
        Some("Top Level Property 1"),
        None,
        Defined,
        Some("PropertyGroup1"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val sHA256ServerSideChecksumField: CustomMetadataField =
      CustomMetadataField(
        "SHA256ServerSideChecksum",
        Some(Description),
        None,
        System,
        Some("System"),
        Text,
        false,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val closureTypeClosedValues: CustomMetadataValues = CustomMetadataValues(List(closurePeriodField, closureStartDateField, titleClosedField), "Closed", 2147483647)
    private val closureTypeOpenValues: CustomMetadataValues = CustomMetadataValues(List(), "Open", 2147483647)

    private val closureTypeField: CustomMetadataField =
      CustomMetadataField(
        ClosureType,
        Some(ClosureType),
        None,
        Defined,
        closurePropertyGroup,
        Text,
        true,
        false,
        Some("Open"),
        List(closureTypeClosedValues, closureTypeOpenValues),
        2147483647,
        false,
        None
      )

    private val descriptionClosedTrueValues: CustomMetadataValues = CustomMetadataValues(List(alternativeDescriptionField), "true", 2147483647)

    private val descriptionClosedField: CustomMetadataField =
      CustomMetadataField(
        DescriptionClosed,
        Some(DescriptionClosed),
        None,
        Defined,
        closurePropertyGroup,
        Boolean,
        true,
        false,
        Some("false"),
        List(descriptionClosedTrueValues),
        2147483647,
        false,
        None
      )

    private val foiExemptionCodeField =
      CustomMetadataField(
        "FoiExemptionCode",
        Some("FOI Exemption Code"),
        None,
        Defined,
        closurePropertyGroup,
        Text,
        true,
        true,
        None,
        List(),
        2147483647,
        false,
        None
      )

    private val mockFields = Future(
      Seq(
        topLevelDependencyField,
        closureStartDateField,
        closurePeriodField,
        titleClosedField,
        closureTypeField,
        descriptionField,
        descriptionClosedField,
        alternativeDescriptionField,
        foiExemptionCodeField,
        sHA256ServerSideChecksumField
      )
    )

    def stubResponse(): Unit = {
      when(customMetadataServiceMock.getCustomMetadata).thenReturn(mockFields)
    }
  }
}
