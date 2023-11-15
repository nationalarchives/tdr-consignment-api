package uk.gov.nationalarchives.tdr.api.service

import cats.implicits.catsSyntaxOptionId
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.DisplaypropertiesRow
import uk.gov.nationalarchives.tdr.api.db.repository.DisplayPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.DisplayPropertiesFields.{Boolean, DisplayPropertyField, Integer, Text}

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getDisplayProperties" should "correctly return sequence of DisplayProperties fields" in {
    val displayPropertiesRepository = mock[DisplayPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        DisplaypropertiesRow(Some("propertyName1"), Some("attribute1"), Some("attribute1Value1"), Some("text")),
        DisplaypropertiesRow(Some("propertyName1"), Some("attribute2"), Some("attribute2Value2"), Some("integer")),
        DisplaypropertiesRow(Some("propertyName2"), Some("attribute1"), Some("attribute1Value3"), Some("boolean")),
        DisplaypropertiesRow(Some("propertyName2"), Some("attribute2"), Some("attribute2Value4"), Some("text"))
      )
    )

    when(displayPropertiesRepository.getDisplayProperties()).thenReturn(mockPropertyResponse)

    val service = new DisplayPropertiesService(displayPropertiesRepository)
    val response: Seq[DisplayPropertyField] = service.getDisplayProperties.futureValue

    response.size should equal(2)
    val property1 = response.find(_.propertyName == "propertyName1").get
    property1.attributes.size should equal(2)
    val property1Attribute1 = property1.attributes.find(_.attribute == "attribute1").get
    property1Attribute1.value should equal(Some("attribute1Value1"))
    property1Attribute1.`type` should equal(Text)
    val property1Attribute2 = property1.attributes.find(_.attribute == "attribute2").get
    property1Attribute2.value should equal(Some("attribute2Value2"))
    property1Attribute2.`type` should equal(Integer)

    val property2 = response.find(_.propertyName == "propertyName2").get
    property2.attributes.size should equal(2)
    val property2Attribute1 = property2.attributes.find(_.attribute == "attribute1").get
    property2Attribute1.value should equal(Some("attribute1Value3"))
    property2Attribute1.`type` should equal(Boolean)
    val property2Attribute2 = property2.attributes.find(_.attribute == "attribute2").get
    property2Attribute2.value should equal(Some("attribute2Value4"))
    property2Attribute2.`type` should equal(Text)
  }

  "getDisplayProperties" should "throw an error if display attribute has no name" in {
    val displayPropertiesRepository = mock[DisplayPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        DisplaypropertiesRow(Some("propertyName"), None, Some("attribute1Value1"), Some("text"))
      )
    )

    when(displayPropertiesRepository.getDisplayProperties()).thenReturn(mockPropertyResponse)

    val service = new DisplayPropertiesService(displayPropertiesRepository)

    val thrownException = intercept[Exception] {
      service.getDisplayProperties.futureValue
    }

    thrownException.getMessage should equal(
      s"The future returned an exception of type: java.lang.Exception, with message: " +
        "Error: Property name 'propertyName' has empty attribute name."
    )
  }

  "getDisplayProperties" should "throw an error if display attribute type is not recognised" in {
    val displayPropertiesRepository = mock[DisplayPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        DisplaypropertiesRow(Some("propertyName"), Some("attribute"), Some("attribute1Value1"), Some("unknownDataType"))
      )
    )

    when(displayPropertiesRepository.getDisplayProperties()).thenReturn(mockPropertyResponse)

    val service = new DisplayPropertiesService(displayPropertiesRepository)

    val thrownException = intercept[Exception] {
      service.getDisplayProperties.futureValue
    }

    thrownException.getMessage should equal(
      s"The future returned an exception of type: java.lang.Exception, with message: " +
        "Invalid data type Some(unknownDataType)."
    )
  }

  "getActiveDisplayPropertyNames" should "return all the active display property names" in {
    val displayPropertiesRepository = mock[DisplayPropertiesRepository]
    val mockPropertyResponse = Future(
      Seq(
        DisplaypropertiesRow(Some("propertyName1"), Some("Active"), Some("true"), Some("boolean")),
        DisplaypropertiesRow(Some("propertyName3"), Some("Active"), Some("true"), Some("boolean"))
      )
    )

    when(displayPropertiesRepository.getDisplayProperties("Active".some, "true".some)).thenReturn(mockPropertyResponse)

    val service = new DisplayPropertiesService(displayPropertiesRepository)
    val response: Seq[String] = service.getActiveDisplayPropertyNames.futureValue

    response should be(List("propertyName1", "propertyName3"))
  }
}
