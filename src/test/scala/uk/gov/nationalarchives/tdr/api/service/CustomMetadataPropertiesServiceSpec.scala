package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getCustomMetadata" should "correctly return sequence of metadataField" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(Seq(
      FilepropertyRow("closureType", None, Some("Closure Type"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
    ))
    val mockPropertyValuesResponse = Future(Seq(
      FilepropertyvaluesRow("closureType", "Closed", None, None, None)
    ))
    val mockPropertyDependenciesResponse = Future(Seq(
      FilepropertydependenciesRow(3, "ClosurePeriod", None)
    ))

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.getCustomMetadata.futureValue

    response.size should equal(1)
    response.head.values.head.value should equal("Closed")
    response.head.values.head.dependencies.isEmpty should equal(true)
  }

  "getCustomMetadata" should "correctly return sequence of metadataField with dependencies" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(Seq(
      FilepropertyRow("closureType", None, Some("Closure Type"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure")),
      FilepropertyRow("ClosurePeriod", None, Some("Closure Type"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"))
    ))
    val mockPropertyValuesResponse = Future(Seq(
      FilepropertyvaluesRow("closureType", "Closed", None, Some(3), None)
    ))
    val mockPropertyDependenciesResponse = Future(Seq(
      FilepropertydependenciesRow(3, "ClosurePeriod", None)
    ))

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.getCustomMetadata.futureValue

    response.size should equal(2)
    response.head.values.size should equal(1)
    response(1).values.size should equal(0)
  }

  "getCustomMetadata" should "return fields in the correct order" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(Seq(
      FilepropertyRow("firstValue", None, Some("First Value"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(1)),
      FilepropertyRow("noOrdering", None, Some("No ordering"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), None),
      FilepropertyRow("thirdValue", None, Some("Third Value"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(3))
    ))
    val mockPropertyValuesResponse = Future(Seq(
      FilepropertyvaluesRow("closureType", "Closed", None, Some(3), None)
    ))
    val mockPropertyDependenciesResponse = Future(Seq(
      FilepropertydependenciesRow(3, "ClosurePeriod", None)
    ))

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.getCustomMetadata.futureValue

    response.size should equal(3)
    val responseMap = response.groupBy(_.name)
    responseMap("firstValue").head.uiOrdinal should equal(1)
    responseMap("noOrdering").head.uiOrdinal should equal(Int.MaxValue)
    responseMap("thirdValue").head.uiOrdinal should equal(3)
  }

  "getCustomMetadata" should "return values in the correct order" in {
    val customMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
    val mockPropertyResponse = Future(Seq(
      FilepropertyRow("firstValue", None, Some("First Value"), Timestamp.from(Instant.now()),
        None, Some("Defined"), Some("text"), Some(true), None, Some("Closure"), Option(1)),
    ))
    val mockPropertyValuesResponse = Future(Seq(
      FilepropertyvaluesRow("firstValue", "orderedOneValue", None, None, None, Some(1)),
      FilepropertyvaluesRow("firstValue", "orderedTwoValue", None, None, None, Some(2)),
      FilepropertyvaluesRow("firstValue", "notOrderedValue", None, None, None, None),
    ))
    val mockPropertyDependenciesResponse = Future(Seq(
      FilepropertydependenciesRow(3, "ClosurePeriod", None)
    ))

    when(customMetadataPropertiesRepository.getCustomMetadataProperty).thenReturn(mockPropertyResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataValues).thenReturn(mockPropertyValuesResponse)
    when(customMetadataPropertiesRepository.getCustomMetadataDependencies).thenReturn(mockPropertyDependenciesResponse)

    val service = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val response = service.getCustomMetadata.futureValue

    response.size should equal(1)
    val responseMap = response.flatMap(_.values).groupBy(_.value)
    responseMap("orderedOneValue").head.uiOrdinal should equal(1)
    responseMap("orderedTwoValue").head.uiOrdinal should equal(2)
    responseMap("notOrderedValue").head.uiOrdinal should equal(Int.MaxValue)
  }
}
