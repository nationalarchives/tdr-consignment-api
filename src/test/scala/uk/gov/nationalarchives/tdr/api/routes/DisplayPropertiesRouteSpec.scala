package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.util.UUID

class DisplayPropertiesRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  sealed trait DataType
  case object Text extends DataType
  case object Integer extends DataType
  case object DateTime extends DataType
  case object Decimal extends DataType

  val consignmentId: UUID = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
  val userId: UUID = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)
  private val displayPropertiesJsonFilePrefix: String = "json/displayproperties_"
  val runDisplayPropertiesTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData =
    runTestRequest[GraphqlQueryData](displayPropertiesJsonFilePrefix)
  val expectedDisplayPropertiesQueryResponse: String => GraphqlQueryData =
    getDataFromFile[GraphqlQueryData](displayPropertiesJsonFilePrefix)

  case class GraphqlQueryData(data: Option[GetDisplayPropertiesFields], errors: List[GraphqlError] = Nil)

  case class GetDisplayPropertiesFields(displayProperties: List[DisplayPropertyField])

  case class DisplayAttribute(attribute: String, value: Option[String], `type`: String)

  case class DisplayPropertyField(
      propertyName: String,
      attributes: List[DisplayAttribute]
  )

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  "displayProperties" should "return all display properties" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val token = validUserToken(userId)

    addDisplayProperties(utils, consignmentId, userId)

    val expectedResponse = expectedDisplayPropertiesQueryResponse("data_all")
    val response = runDisplayPropertiesTestQuery("query_alldata", token)
    response.data.get.displayProperties should equal(expectedResponse.data.get.displayProperties)
  }

  "displayProperties" should "return all requested fields" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val token = validUserToken(userId)

    addDisplayProperties(utils, consignmentId, userId)

    val expectedResponse = expectedDisplayPropertiesQueryResponse("data_some")
    val response = runDisplayPropertiesTestQuery("query_somedata", token)

    response.data.get.displayProperties should equal(expectedResponse.data.get.displayProperties)
  }

  "displayProperties" should "return an error if the consignmentId was not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val token = validUserToken(userId)

    addDisplayProperties(utils, consignmentId, userId)

    val expectedResponse = expectedDisplayPropertiesQueryResponse("data_error_no_consignmentid")
    val response = runDisplayPropertiesTestQuery("query_no_consignmentid", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "displayProperties" should "return an error if the consignmentId provided was not valid" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val token = validUserToken(userId)

    addDisplayProperties(utils, consignmentId, userId)

    val expectedResponse = expectedDisplayPropertiesQueryResponse("data_invalid_consignmentid")
    val response = runDisplayPropertiesTestQuery("query_invalid_consignmentid", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def addDisplayProperties(utils: TestUtils, consignmentId: UUID, userId: UUID): Unit = {
    utils.createConsignment(consignmentId, userId)
    utils.createFileProperty("testProperty1", "description", "Defined", "text", true, false, "group", "Language")
    utils.createFileProperty("testProperty2", "description", "Defined", "text", true, false, "group", "Language")

    utils.createDisplayProperty("testProperty1", "attribute1", "attributeValue1", "text")
    utils.createDisplayProperty("testProperty1", "attribute2", "attributeValue2", "boolean")
    utils.createDisplayProperty("testProperty2", "attribute1", "attributeValue1", "text")
    utils.createDisplayProperty("testProperty2", "attribute2", "attributeValue2", "boolean")
  }
}
