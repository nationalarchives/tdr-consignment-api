package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ClosureMetadata, DescriptiveMetadata}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ValidateFileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fixedTimeSource: FixedTimeSource.type = FixedTimeSource

  "validateAdditionalMetadata" should "not update 'additional metadata' statuses if updated properties are not 'additional metadata' properties" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    testSetUp.stubMockResponses(fileIds)
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(testSetUp.mockCustomMetadataService, testSetUp.mockFileMetadataRepository, mockFileStatusRepository, fixedTimeSource)
    val response = service.validateAdditionalMetadata(fileIds, testSetUp.consignmentId, Set("nonAdditionalMetadataProperty")).futureValue

    response.size shouldBe 0

    verify(mockFileStatusRepository, times(0)).deleteFileStatus(any[Set[UUID]], any[Set[String]])
    verify(mockFileStatusRepository, times(0)).addFileStatuses(any[List[FilestatusRow]])
  }

  "validateAdditionalMetadata" should "update 'ClosureMetadata' status to 'Complete' for multiple files where there are no missing dependencies" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "someDate", Timestamp.from(FixedTimeSource.now), userId, "ClosurePeriod"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "30", Timestamp.from(FixedTimeSource.now), userId, "FoiExemptionCode"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "true", Timestamp.from(FixedTimeSource.now), userId, "TitleClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "alternative title", Timestamp.from(FixedTimeSource.now), userId, "TitleAlternate"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "false", Timestamp.from(FixedTimeSource.now), userId, "DescriptionClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someDate", Timestamp.from(FixedTimeSource.now), userId, "ClosurePeriod"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "30", Timestamp.from(FixedTimeSource.now), userId, "FoiExemptionCode"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "true", Timestamp.from(FixedTimeSource.now), userId, "TitleClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "alternative title", Timestamp.from(FixedTimeSource.now), userId, "TitleAlternate"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "false", Timestamp.from(FixedTimeSource.now), userId, "DescriptionClosed")
    )

    testSetUp.stubMockResponses(fileIds, existingMetadataRows)
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(testSetUp.mockCustomMetadataService, testSetUp.mockFileMetadataRepository, mockFileStatusRepository, fixedTimeSource)
    val response = service.validateAdditionalMetadata(fileIds, testSetUp.consignmentId, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    verify(mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(mockFileStatusRepository, times(1)).addFileStatuses(response)

    val file1Statuses = response.filter(_.fileid == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("Completed")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("Completed")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAdditionalMetadata" should "update 'ClosureMetadata' status to 'Incomplete' for multiple files where there are missing dependencies" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "English", Timestamp.from(FixedTimeSource.now), userId, "Language"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "English", Timestamp.from(FixedTimeSource.now), userId, "Language")
    )

    testSetUp.stubMockResponses(fileIds, existingMetadataRows)
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(testSetUp.mockCustomMetadataService, testSetUp.mockFileMetadataRepository, mockFileStatusRepository, fixedTimeSource)
    val response = service.validateAdditionalMetadata(fileIds, testSetUp.consignmentId, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    verify(mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(mockFileStatusRepository, times(1)).addFileStatuses(response)

    val file1Statuses = response.filter(_.fileid == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("Incomplete")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("Incomplete")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAdditionalMetadata" should "update 'ClosureMetadata' status to 'NotEntered' for multiple files where all existing property values match defaults" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "Open", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Open", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "English", Timestamp.from(FixedTimeSource.now), userId, "Language"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "English", Timestamp.from(FixedTimeSource.now), userId, "Language")
    )

    testSetUp.stubMockResponses(fileIds, existingMetadataRows)
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(testSetUp.mockCustomMetadataService, testSetUp.mockFileMetadataRepository, mockFileStatusRepository, fixedTimeSource)
    val response = service.validateAdditionalMetadata(fileIds, testSetUp.consignmentId, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    verify(mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(mockFileStatusRepository, times(1)).addFileStatuses(response)

    val file1Statuses = response.filter(_.fileid == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("NotEntered")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("NotEntered")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAdditionalMetadata" should "update 'ClosureMetadata' and 'DescriptiveMetadata' statuses to 'NotEntered' for multiple files where there are no existing metadata properties" in {
    val mockFileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val testSetUp = new ValidatePropertiesSetUp()
    val fileIds = Set(testSetUp.fileId1, testSetUp.fileId2)
    testSetUp.stubMockResponses(fileIds)
    when(mockFileMetadataRepository.getFileMetadata(any[UUID], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(Future(List()))

    val service = new ValidateFileMetadataService(testSetUp.mockCustomMetadataService, mockFileMetadataRepository, mockFileStatusRepository, fixedTimeSource)
    val response = service.validateAdditionalMetadata(fileIds, testSetUp.consignmentId, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    verify(mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(mockFileStatusRepository, times(1)).addFileStatuses(response)

    val file1Statuses = response.filter(_.fileid == testSetUp.fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("NotEntered")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == testSetUp.fileId2)
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("NotEntered")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAdditionalMetadata" should "update 'ClosureMetadata' status to 'Incomplete' for multiple files where all values set to default but dependency is missing" in {}

  "validateAdditionalMetadata" should "update 'DescriptiveMetadata' status to 'Complete' for multiple files where there are no missing dependencies" in {}

  "checkPropertyState" should "return the property correct states for multiple files where the property value has no dependencies" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2
    val fieldToValidate = testSetup.noDependenciesField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "someValue", Timestamp.from(FixedTimeSource.now), userId, "NoDependencies"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someValue", Timestamp.from(FixedTimeSource.now), userId, "NoDependencies")
    )

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1, fileId2), fieldToValidate, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.missingDependencies shouldBe false
    file1State.propertyName should equal(fieldToValidate.name)
    file1State.existingValueMatchesDefault shouldBe None
    val file2State = response.find(_.fileId == fileId2).get
    file2State.missingDependencies shouldBe false
    file2State.propertyName should equal(fieldToValidate.name)
    file2State.existingValueMatchesDefault shouldBe None
  }

  "checkPropertyState" should "return the correct property states for multiple files where one file is missing a dependency" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val fieldToCheck = testSetup.hasDependenciesField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "value1", Timestamp.from(FixedTimeSource.now), userId, "Dependent"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "aValue", Timestamp.from(FixedTimeSource.now), userId, "HasDependencies"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "aValue", Timestamp.from(FixedTimeSource.now), userId, "HasDependencies")
    )

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1, fileId2), fieldToCheck, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.missingDependencies shouldBe false
    file1State.propertyName should equal(fieldToCheck.name)
    file1State.existingValueMatchesDefault shouldBe None
    val file2State = response.find(_.fileId == fileId2).get
    file2State.missingDependencies shouldBe true
    file2State.propertyName should equal(fieldToCheck.name)
    file2State.existingValueMatchesDefault shouldBe None
  }

  "checkPropertyState" should "return the correct property states for multiple files where property is multi-value with no dependencies" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val fieldToCheck = testSetup.multiValueField1

    val existingMetadata: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "40", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "30", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "40", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "30", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty")
    )

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1, fileId2), fieldToCheck, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.missingDependencies shouldBe false
    file1State.propertyName should equal(fieldToCheck.name)
    file1State.existingValueMatchesDefault shouldBe None
    val file2State = response.find(_.fileId == fileId2).get
    file2State.missingDependencies shouldBe false
    file2State.propertyName should equal(fieldToCheck.name)
    file2State.existingValueMatchesDefault shouldBe None
  }

  "checkPropertyState" should "return the no property states where property does not exist for file" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "someValue", Timestamp.from(FixedTimeSource.now), userId, "someOtherProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someValue", Timestamp.from(FixedTimeSource.now), userId, "someOtherProperty")
    )

    val fieldToCheck = testSetup.noDependenciesField1

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1, fileId2), fieldToCheck, existingMetadata)

    response.size shouldBe 0
  }

  "checkPropertyState" should "return the correct property states where default property value is different from existing property value" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fieldToCheck = testSetup.hasDefaultField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "valueOtherThanDefault", Timestamp.from(FixedTimeSource.now), userId, "HasDefaultField")
    )

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1), fieldToCheck, existingMetadata)

    response.size shouldBe 1
    val state = response.head
    state.fileId should equal(fileId1)
    state.propertyName should equal(fieldToCheck.name)
    state.missingDependencies shouldBe false
    state.existingValueMatchesDefault.get shouldBe false
  }

  "checkPropertyState" should "return the correct property states where default property value is same as existing value" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1

    val fieldToCheck = testSetup.hasDefaultField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, fieldToCheck.defaultValue.get, Timestamp.from(FixedTimeSource.now), userId, "HasDefaultField")
    )

    val customMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val fileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val fileStatusRepository: FileStatusRepository = mock[FileStatusRepository]

    val service = new ValidateFileMetadataService(customMetadataService, fileMetadataRepository, fileStatusRepository, fixedTimeSource)
    val response = service.checkPropertyState(Set(fileId1), fieldToCheck, existingMetadata)

    response.size shouldBe 1
    val state = response.head
    state.fileId should equal(fileId1)
    state.propertyName should equal(fieldToCheck.name)
    state.missingDependencies shouldBe false
    state.existingValueMatchesDefault.get shouldBe true
  }

  private class ValidatePropertiesSetUp() {
    val userId: UUID = UUID.randomUUID()
    val consignmentId: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()

    val mockCustomMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val mockFileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val mockFields = mockCustomMetadataFields()
    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset

    def stubMockResponses(fileIds: Set[UUID] = Set(fileId1), metadataRows: List[FilemetadataRow] = List()): Unit = {
      when(mockCustomMetadataService.getCustomMetadata).thenReturn(Future(mockFields))
      when(mockFileMetadataRepository.getFileMetadata(any[UUID], any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(Future(metadataRows))
    }
  }

  private def mockCustomMetadataFields(): Seq[CustomMetadataField] = {
    val closurePeriodField: CustomMetadataField =
      CustomMetadataField("ClosurePeriod", Some("Closure Period"), None, Defined, Some("MandatoryClosure"), Text, true, false, None, List(), 2147483647, false, None)
    val descriptionField: CustomMetadataField =
      CustomMetadataField("description", Some("description"), None, Defined, Some("OptionalMetadata"), Text, true, false, None, List(), 2147483647, false, None)
    val languageField =
      CustomMetadataField("Language", Some("Language"), None, Defined, Some("OptionalMetadata"), Text, true, true, Some("English"), List(), 2147483647, false, None)
    val alternativeDescriptionField: CustomMetadataField =
      CustomMetadataField(
        "AlternativeDescription",
        Some("Alternative Description"),
        None,
        Defined,
        Some("OptionalClosure"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )
    val alternativeTitleField: CustomMetadataField =
      CustomMetadataField("TitleAlternate", Some("Alternative Title"), None, Defined, Some("OptionalClosure"), Text, true, false, None, List(), 2147483647, false, None)
    val foiExemptionCode40Value: CustomMetadataValues = CustomMetadataValues(List(closurePeriodField), "40", 2147483647)
    val foiExemptionCode30Value: CustomMetadataValues = CustomMetadataValues(List(closurePeriodField), "30", 2147483647)
    val foiExemptionCodeField =
      CustomMetadataField(
        "FoiExemptionCode",
        Some("FOI Exemption Code"),
        None,
        Defined,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        None,
        List(foiExemptionCode40Value, foiExemptionCode30Value),
        2147483647,
        false,
        None
      )

    val descriptionClosedTrueValues: CustomMetadataValues = CustomMetadataValues(List(alternativeDescriptionField), "true", 2147483647)
    val descriptionClosedFalseValues: CustomMetadataValues = CustomMetadataValues(List(), "false", 2147483647)
    val descriptionClosedField: CustomMetadataField =
      CustomMetadataField(
        "DescriptionClosed",
        Some("DescriptionClosed"),
        None,
        Supplied,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        Some("false"),
        List(descriptionClosedTrueValues, descriptionClosedFalseValues),
        2147483647,
        false,
        None
      )

    val titleClosedTrueValue: CustomMetadataValues = CustomMetadataValues(List(alternativeTitleField), "true", 2147483647)
    val titleClosedFalseValue: CustomMetadataValues = CustomMetadataValues(List(), "false", 2147483647)
    val titleClosedField: CustomMetadataField =
      CustomMetadataField(
        "TitleClosed",
        Some("TitleClosed"),
        None,
        Supplied,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        Some("false"),
        List(titleClosedTrueValue, titleClosedFalseValue),
        2147483647,
        false,
        None
      )

    val closureTypeClosedValues: CustomMetadataValues =
      CustomMetadataValues(List(closurePeriodField, foiExemptionCodeField, descriptionClosedField, titleClosedField), "Closed", 2147483647)
    val closureTypeOpenValues: CustomMetadataValues = CustomMetadataValues(List(), "Open", 2147483647)
    val closureTypeField: CustomMetadataField =
      CustomMetadataField(
        "ClosureType",
        Some("Closure Type"),
        None,
        Defined,
        Some("MandatoryClosure"),
        Text,
        true,
        false,
        Some("Open"),
        List(closureTypeClosedValues, closureTypeOpenValues),
        2147483647,
        false,
        None
      )

    Seq(
      closurePeriodField,
      closureTypeField,
      descriptionField,
      alternativeDescriptionField,
      foiExemptionCodeField,
      languageField,
      titleClosedField,
      descriptionClosedField,
      alternativeTitleField
    )
  }

  private class ValidatePropertySetUp() {
    val userId: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()

    val multiValueField1: CustomMetadataField =
      CustomMetadataField("MultiValueProperty", Some("MultiValue Property"), None, Defined, Some("Group"), Text, true, true, None, List(), 2147483647, false, None)
    val noDependenciesField1: CustomMetadataField =
      CustomMetadataField("NoDependencies", Some("No Dependencies"), None, Defined, Some("Group"), Text, true, false, None, List(), 2147483647, false, None)
    private val dependentField1: CustomMetadataField =
      CustomMetadataField("Dependent", Some("Dependent"), None, Defined, Some("Group"), Text, true, false, None, List(), 2147483647, false, None)
    private val customValues1: CustomMetadataValues = CustomMetadataValues(List(dependentField1), "aValue", 2147483647)

    val hasDependenciesField1: CustomMetadataField =
      CustomMetadataField("HasDependencies", Some("Has Dependencies"), None, Defined, Some("Group"), Text, true, false, None, List(customValues1), 2147483647, false, None)

    val hasDefaultField1: CustomMetadataField =
      CustomMetadataField("HasDefaultField", Some("Has Default Field"), None, Defined, Some("Group"), Text, true, false, Some("defaultValue"), List(), 2147483647, false, None)
  }
}
