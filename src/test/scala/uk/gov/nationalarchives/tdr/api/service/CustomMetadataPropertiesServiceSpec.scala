package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.CustomMetadataField
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ClosureMetadata, DescriptiveMetadata}
import uk.gov.nationalarchives.tdr.api.utils.TestDataHelper.mockCustomMetadataFields

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "toAdditionalMetadataFieldGroups" should "group 'custom metadata fields' by 'closure' and 'descriptive'" in {

    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)

    val fieldGroups = service.toAdditionalMetadataFieldGroups(mockCustomMetadataFields())

    fieldGroups.size shouldBe 2
    val closureGroups = fieldGroups.find(_.groupName == ClosureMetadata).get
    closureGroups.fields.size shouldBe 7
    val descriptiveGroups = fieldGroups.find(_.groupName == DescriptiveMetadata).get
    descriptiveGroups.fields.size shouldBe 2
  }

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
}
