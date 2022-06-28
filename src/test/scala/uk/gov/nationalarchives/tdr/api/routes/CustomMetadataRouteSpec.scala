package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._

import java.util.UUID

class CustomMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  sealed trait DataType
  case object Text extends DataType
  case object Integer extends DataType
  case object DateTime extends DataType
  case object Decimal extends DataType

  sealed trait PropertyType
  case object System extends PropertyType
  case object Defined extends PropertyType
  case object Supplied extends PropertyType

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)
  private val closureMetadataJsonFilePrefix: String = "json/closuremetadata_"
  val runClosureMetadataTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData =
    runTestRequest[GraphqlQueryData](closureMetadataJsonFilePrefix)
  val expectedClosureMetadataQueryResponse: String => GraphqlQueryData =
    getDataFromFile[GraphqlQueryData](closureMetadataJsonFilePrefix)

  case class GraphqlQueryData(data: Option[GetCustomMetadataFields], errors: List[GraphqlError] = Nil)

  case class GetCustomMetadataFields(closureMetadata: List[CustomMetadataFields])

  case class CustomMetadataFields(name: String,
                                  fullName: Option[String],
                                  description: Option[String],
                                  propertyGroup: Option[String],
                                  editable: Boolean,
                                  multiValue: Boolean,
                                  propertyType: String,
                                  dataType: String,
                                  defaultValue: Option[String],
                                  values: Option[List[CustomMetadataValues]]
                                )

  case class CustomMetadataValues(dependencies: List[CustomMetadataFields], value: String)

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  "closureMetadata" should "return all of the closure metadata with the correct arguments" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)

      addDummyFilePropertiesAndValuesToDb(utils, consignmentId, userId)

      val expectedResponse = expectedClosureMetadataQueryResponse("data_all")
      val response = runClosureMetadataTestQuery("query_alldata", token)

      response.data.get.closureMetadata.head should equal(expectedResponse.data.get.closureMetadata.head)
  }

  "closureMetadata" should "return all requested fields" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)

      addDummyFilePropertiesAndValuesToDb(utils, consignmentId, userId)

      val expectedResponse = expectedClosureMetadataQueryResponse("data_some")
      val response = runClosureMetadataTestQuery("query_somedata", token)

      response.data.get.closureMetadata.head should equal(expectedResponse.data.get.closureMetadata.head)
  }

  "closureMetadata" should "return an error if the consignmentId was not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)

      addDummyFilePropertiesAndValuesToDb(utils, consignmentId, userId)

      val expectedResponse = expectedClosureMetadataQueryResponse("data_error_no_consignmentid")
      val response = runClosureMetadataTestQuery("query_no_consignmentid", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "closureMetadata" should "return an error if the consignmentId provided was not valid" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)

      addDummyFilePropertiesAndValuesToDb(utils, consignmentId, userId)

      val expectedResponse = expectedClosureMetadataQueryResponse("data_invalid_consignmentid")
      val response = runClosureMetadataTestQuery("query_invalid_consignmentid", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def addDummyFilePropertiesAndValuesToDb(utils: TestUtils, consignmentId: UUID, userId: UUID): Unit = {
    utils.createConsignment(consignmentId, userId)
    utils.createFileProperty(
      "TestProperty",
      "It's the Test Property",
      "Defined",
      "text",
      editable = false,
      multivalue = false,
      "Test Property Group",
      "Test Property"
    )

    utils.createFileProperty(
      "TestDependency",
      "It's the Test Dependency",
      "Defined",
      "text",
      editable = false,
      multivalue = false,
      "Test Dependency Group",
      "Test Dependency"
    )

    utils.createFilePropertyValues("TestProperty", "TestValue", default = true, 2, 1)
    utils.createFilePropertyDependencies(2, "TestDependency", "TestDependencyValue")
  }
 }
