package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues, Defined, Text}
import uk.gov.nationalarchives.tdr.api.utils.FixedTimeSource

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getCustomMetadata" should "correctly return sequence of CustomMetadataField without dependencies" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("closureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("closureType", "Closed", None, None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response: Seq[CustomMetadataField] = service.getCustomMetadata.futureValue

    response.size should equal(2)
    response.head.values.head.value should equal("Closed")
    response.head.values.head.dependencies.isEmpty should equal(true)
  }

  "getCustomMetadata" should "correctly return sequence of CustomMetadataField with dependencies" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("closureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Period"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("closureType", "Closed", None, Some(3), None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response: Seq[CustomMetadataField] = service.getCustomMetadata.futureValue

    response.size should equal(2)
    response.head.values.size should equal(1)
    response(1).values.size should equal(0)
  }

  "getCustomMetadata" should "throw an error if propertyName of a value is not present in FileProperty table" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow(
          "propertyNameNotInValues",
          None,
          Some("Closure Type"),
          Timestamp.from(Instant.now()),
          None,
          Some("Defined"),
          Some("text"),
          Some(true),
          None,
          Some("Closure")
        ),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("closureType", "Closed", None, Some(3), None),
        FilepropertyvaluesRow("ClosurePeriod", "0", None, None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)

    val thrownException = intercept[Exception] {
      service.getCustomMetadata.futureValue
    }

    thrownException.getMessage should equal(
      s"The future returned an exception of type: java.lang.Exception, with message: " +
        "Error: Property name closureType, in the values table, does not exist in the FileProperty table."
    )
  }

  "getCustomMetadata" should "throw an error if dependency name is not present in FileProperty table" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("ClosurePeriod", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("ClosurePeriod", "0", None, None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None),
        FilepropertydependenciesRow(3, "dependencyNameNotInFileProperty", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)

    val thrownException = intercept[Exception] {
      service.getCustomMetadata.futureValue
    }

    thrownException.getMessage should equal(
      s"The future returned an exception of type: java.lang.Exception, with message: " +
        "Error: Property name dependencyNameNotInFileProperty, in the dependencies table, does not exist in the FileProperty table."
    )
  }

  "getCustomMetadata" should "return fields in the correct order" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("firstValue", None, Some("First Value"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(1)),
        FilepropertyRow("noOrdering", None, Some("No ordering"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), None),
        FilepropertyRow("thirdValue", None, Some("Third Value"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(3)),
        FilepropertyRow("closureType", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), None),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), None)
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("closureType", "Closed", None, Some(3), None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response: Seq[CustomMetadataField] = service.getCustomMetadata.futureValue

    response.size should equal(5)
    val responseMap = response.groupBy(_.name)
    responseMap("firstValue").head.uiOrdinal should equal(1)
    responseMap("noOrdering").head.uiOrdinal should equal(Int.MaxValue)
    responseMap("thirdValue").head.uiOrdinal should equal(3)
    responseMap("closureType").head.uiOrdinal should equal(Int.MaxValue)
    responseMap("ClosurePeriod").head.uiOrdinal should equal(Int.MaxValue)
  }

  "getCustomMetadata" should "return values in the correct order" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        FilepropertyRow("firstValue", None, Some("First Value"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(1)),
        FilepropertyRow("ClosurePeriod", None, Some("Closure Type"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
      )
    )
    val mockPropertyValuesResponse = Future(
      Seq(
        FilepropertyvaluesRow("firstValue", "orderedOneValue", None, None, None, Some(1)),
        FilepropertyvaluesRow("firstValue", "orderedTwoValue", None, None, None, Some(2)),
        FilepropertyvaluesRow("firstValue", "notOrderedValue", None, None, None, None)
      )
    )
    val mockPropertyDependenciesResponse = Future(
      Seq(
        FilepropertydependenciesRow(3, "ClosurePeriod", None)
      )
    )

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response: Seq[CustomMetadataField] = service.getCustomMetadata.futureValue

    response.size should equal(2)
    val responseMap = response.flatMap(_.values).groupBy(_.value)
    responseMap("orderedOneValue").head.uiOrdinal should equal(1)
    responseMap("orderedTwoValue").head.uiOrdinal should equal(2)
    responseMap("notOrderedValue").head.uiOrdinal should equal(Int.MaxValue)
  }

  "validateProperty" should "return the property correct states for multiple files where property value has no dependencies" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2
    val fieldToValidate = testSetup.noDependenciesField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "someValue", Timestamp.from(FixedTimeSource.now), userId, "NoDependencies"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someValue", Timestamp.from(FixedTimeSource.now), userId, "NoDependencies")
    )

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1, fileId2), fieldToValidate, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.valid shouldBe true
    file1State.propertyName should equal(fieldToValidate.name)
    file1State.defaultDoesNotMatchExistingValue shouldBe false
    val file2State = response.find(_.fileId == fileId2).get
    file2State.valid shouldBe true
    file2State.propertyName should equal(fieldToValidate.name)
    file2State.defaultDoesNotMatchExistingValue shouldBe false
  }

  "validateProperty" should "return the correct property states for multiple files where one file is missing a dependency" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val fieldToValidate = testSetup.hasDependenciesField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "value1", Timestamp.from(FixedTimeSource.now), userId, "Dependent"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "aValue", Timestamp.from(FixedTimeSource.now), userId, "HasDependencies"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "aValue", Timestamp.from(FixedTimeSource.now), userId, "HasDependencies")
    )

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1, fileId2), fieldToValidate, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.valid shouldBe true
    file1State.propertyName should equal(fieldToValidate.name)
    file1State.defaultDoesNotMatchExistingValue shouldBe false
    val file2State = response.find(_.fileId == fileId2).get
    file2State.valid shouldBe false
    file2State.propertyName should equal(fieldToValidate.name)
    file2State.defaultDoesNotMatchExistingValue shouldBe false
  }

  "validateProperty" should "return the correct property states for multiple files where property is multi-value with no dependencies" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val fieldToValidate = testSetup.multiValueField1

    val existingMetadata: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "40", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "30", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "40", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "30", Timestamp.from(FixedTimeSource.now), userId, "MultiValueProperty")
    )

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1, fileId2), fieldToValidate, existingMetadata)

    response.size shouldBe 2
    val file1State = response.find(_.fileId == fileId1).get
    file1State.valid shouldBe true
    file1State.propertyName should equal(fieldToValidate.name)
    file1State.defaultDoesNotMatchExistingValue shouldBe false
    val file2State = response.find(_.fileId == fileId2).get
    file2State.valid shouldBe true
    file2State.propertyName should equal(fieldToValidate.name)
    file2State.defaultDoesNotMatchExistingValue shouldBe false
  }

  "validateProperty" should "return the no property states where property does not exist for file" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fileId2: UUID = testSetup.fileId2

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, "someValue", Timestamp.from(FixedTimeSource.now), userId, "someOtherProperty"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someValue", Timestamp.from(FixedTimeSource.now), userId, "someOtherProperty")
    )

    val fieldToValidate = testSetup.noDependenciesField1

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1, fileId2), fieldToValidate, existingMetadata)

    response.size shouldBe 0
  }

  "validateProperty" should "return the correct property states where default property value is different from existing property value" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1
    val fieldToValidate = testSetup.hasDefaultField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, fieldToValidate.defaultValue.get, Timestamp.from(FixedTimeSource.now), userId, "HasDefaultField")
    )

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1), fieldToValidate, existingMetadata)

    response.size shouldBe 1
    val state = response.head
    state.fileId should equal(fileId1)
    state.propertyName should equal(fieldToValidate.name)
    state.valid shouldBe true
    state.defaultDoesNotMatchExistingValue shouldBe false
  }

  "validateProperty" should "return the correct property states where default property value is same as existing value" in {
    val testSetup = new ValidatePropertySetUp()
    val userId: UUID = testSetup.userId
    val fileId1: UUID = testSetup.fileId1

    val fieldToValidate = testSetup.hasDefaultField1

    val existingMetadata: Seq[FilemetadataRow] = Seq(
      FilemetadataRow(UUID.randomUUID(), fileId1, fieldToValidate.defaultValue.get, Timestamp.from(FixedTimeSource.now), userId, "HasDefaultField")
    )

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.validateProperty(Set(fileId1), fieldToValidate, existingMetadata)

    response.size shouldBe 1
    val state = response.head
    state.fileId should equal(fileId1)
    state.propertyName should equal(fieldToValidate.name)
    state.valid shouldBe true
    state.defaultDoesNotMatchExistingValue shouldBe false
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
