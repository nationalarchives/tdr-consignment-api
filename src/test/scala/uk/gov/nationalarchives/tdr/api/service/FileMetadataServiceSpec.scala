package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.Tables.FilemetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{Boolean, CustomMetadataField, CustomMetadataValues, Defined, Supplied, System, Text}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{SHA256ServerSideChecksum, _}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
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
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
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

    val service = new FileMetadataService(metadataRepositoryMock, customMetadataServiceMock)
    service.addFileMetadata(createInput(SHA256ServerSideChecksum, fixedFileUuid, "value"), fixedUserId).futureValue

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
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val mockMetadataResponse = Future.successful(
      FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "value", Timestamp.from(FixedTimeSource.now), fixedUserId, SHA256ServerSideChecksum) :: Nil
    )
    val propertyName = SHA256ServerSideChecksum
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val mockClientChecksumRow = FilemetadataRow(UUID.randomUUID(), fixedFileUuid, "ChecksumMatch", timestamp, fixedUserId, SHA256ClientSideChecksum)
    val mockClientChecksumResponse = Future(Seq(mockClientChecksumRow))

    when(metadataRepositoryMock.addFileMetadata(any[List[AddFileMetadataInput]])).thenReturn(mockMetadataResponse)
    when(metadataRepositoryMock.getFileMetadataByProperty(fixedFileUuid :: Nil, SHA256ClientSideChecksum)).thenReturn(mockClientChecksumResponse)

    val service = new FileMetadataService(metadataRepositoryMock, customMetadataServiceMock)
    val result: FileMetadataWithFileId =
      service.addFileMetadata(createInput(propertyName, fixedFileUuid, "value"), fixedUserId).futureValue.head

    result.fileId should equal(fixedFileUuid)
    result.filePropertyName should equal(propertyName)
    result.value should equal(value)
  }

  "addOrUpdateBulkFileMetadata" should "delete existing metadata rows and add new metadata rows based on the input" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, customMetadataSetUp.customMetadataServiceMock)

    val addOrUpdateBulkFileMetadata = testSetUp.inputFileIds.map(fileId =>
      AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", "newValue2")))
    )

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)

    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue
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

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, customMetadataSetUp.customMetadataServiceMock)

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

    thrownException.getMessage should include("Protected metadata property found: SHA256ServerSideChecksum")
  }

  "addOrUpdateBulkFileMetadata" should "delete properties with empty values but not add empty properties" in {
    val testSetUp = new AddOrUpdateBulkMetadataTestSetUp()
    val customMetadataSetUp = new CustomMetadataTestSetUp()

    testSetUp.stubRepoResponses()
    customMetadataSetUp.stubResponse()

    val service = new FileMetadataService(testSetUp.metadataRepositoryMock, customMetadataSetUp.customMetadataServiceMock)

    val addOrUpdateBulkFileMetadata =
      testSetUp.inputFileIds.map(fileId => AddOrUpdateFileMetadata(fileId, List(AddOrUpdateMetadata("propertyName1", "newValue1"), AddOrUpdateMetadata("propertyName2", ""))))

    val input = AddOrUpdateBulkFileMetadataInput(testSetUp.consignmentId, addOrUpdateBulkFileMetadata)

    service.addOrUpdateBulkFileMetadata(input, testSetUp.userId).futureValue
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

  "getFileMetadata" should "call the repository with the correct arguments" in {
    val fileMetadataRepositoryMock = mock[FileMetadataRepository]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val consignmentIdCaptor: ArgumentCaptor[Option[UUID]] = ArgumentCaptor.forClass(classOf[Option[UUID]])
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()
    val mockResponse = Future(Seq())
    when(fileMetadataRepositoryMock.getFileMetadata(any[Option[UUID]], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(mockResponse)

    val service =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)

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
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
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
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
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
    val consignmentId = UUID.randomUUID()
    when(fileMetadataRepository.getSumOfFileSizes(any[UUID])).thenReturn(Future(1L))

    val service = new FileMetadataService(fileMetadataRepository, mock[CustomMetadataPropertiesService])
    val result = service.getSumOfFileSizes(consignmentId).futureValue

    result should equal(1)
    verify(fileMetadataRepository, times(1)).getSumOfFileSizes(consignmentId)
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

    val deletePropertyNamesCaptor: ArgumentCaptor[Set[String]] = ArgumentCaptor.forClass(classOf[Set[String]])
    val addFileMetadataCaptor: ArgumentCaptor[Seq[AddFileMetadataInput]] = ArgumentCaptor.forClass(classOf[Seq[AddFileMetadataInput]])
    val deleteFileMetadataIdsArgCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    def stubRepoResponses(
        deleteFileMetadataResponse: Int = 0,
        addFileMetadataResponse: Seq[FilemetadataRow] = Seq()
    ): Unit = {

      when(metadataRepositoryMock.deleteFileMetadata(deleteFileMetadataIdsArgCaptor.capture(), deletePropertyNamesCaptor.capture()))
        .thenReturn(Future(deleteFileMetadataResponse))
      when(metadataRepositoryMock.addFileMetadata(addFileMetadataCaptor.capture()))
        .thenReturn(Future(addFileMetadataResponse))
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
