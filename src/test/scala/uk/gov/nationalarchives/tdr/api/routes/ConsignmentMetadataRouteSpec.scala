package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentMetadataFields.ConsignmentMetadataWithConsignmentId
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.{userId, validUserToken}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{GraphqlError, getDataFromFile}
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class ConsignmentMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addOrUpdateConsignmentMetadataJsonFilePrefix: String = "json/addorupdateconsignmentmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val consignmentProperties: Seq[String] = List("JudgmentType", "PublicRecordsConfirmed")

  case class AddAddOrUpdateConsignmentMetadata(addOrUpdateConsignmentMetadata: List[ConsignmentMetadataWithConsignmentId]) extends TestRequest
  case class GraphqlAddOrUpdateConsignmentMetadataMutationData(data: Option[AddAddOrUpdateConsignmentMetadata], errors: List[GraphqlError] = Nil)

  val runAddOrUpdateConsignmentMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddOrUpdateConsignmentMetadataMutationData =
    runTestRequest[GraphqlAddOrUpdateConsignmentMetadataMutationData](addOrUpdateConsignmentMetadataJsonFilePrefix)
  val expectedAddOrUpdateConsignmentMetadataMutationResponse: String => GraphqlAddOrUpdateConsignmentMetadataMutationData =
    getDataFromFile[GraphqlAddOrUpdateConsignmentMetadataMutationData](addOrUpdateConsignmentMetadataJsonFilePrefix)

  "addOrUpdateConsignmentMetadata" should "return all requested fields and add or update consignment metadata in the DB" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    consignmentProperties.foreach(utils.addConsignmentProperty)
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    utils.createConsignment(consignmentId, userId)
    utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, "JudgmentType", "press_summary")

    val expectedResponse: GraphqlAddOrUpdateConsignmentMetadataMutationData = expectedAddOrUpdateConsignmentMetadataMutationResponse("data_all")
    val response: GraphqlAddOrUpdateConsignmentMetadataMutationData = runAddOrUpdateConsignmentMetadataTestMutation("mutation_alldata", validUserToken())

    response.data.get.addOrUpdateConsignmentMetadata should equal(expectedResponse.data.get.addOrUpdateConsignmentMetadata)
    checkConsignmentMetadataExists(consignmentId, utils, Seq("Judgment", "true"))
  }

  "addOrUpdateConsignmentMetadata" should "throw an error if the consignment id field is not provided" in withContainers { case _: PostgreSQLContainer =>
    val expectedResponse: GraphqlAddOrUpdateConsignmentMetadataMutationData = expectedAddOrUpdateConsignmentMetadataMutationResponse("data_consignmentid_missing")
    val response: GraphqlAddOrUpdateConsignmentMetadataMutationData = runAddOrUpdateConsignmentMetadataTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addOrUpdateConsignmentMetadata" should "return an error if a user does not own the consignment" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val fixedUUIDSource = new FixedUUIDSource()
    val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
    val consignmentId: UUID = fixedUUIDSource.uuid
    utils.createConsignment(consignmentId, otherUserId)

    val expectedResponse: GraphqlAddOrUpdateConsignmentMetadataMutationData = expectedAddOrUpdateConsignmentMetadataMutationResponse("data_error_not_owner")
    val response: GraphqlAddOrUpdateConsignmentMetadataMutationData = runAddOrUpdateConsignmentMetadataTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  private def checkConsignmentMetadataExists(consignmentId: UUID, utils: TestUtils, expectedPropertyValues: Seq[String]): Unit = {
    val placeholders = List.fill(consignmentProperties.size)("?").mkString(",")
    val sql = s"""SELECT * FROM "ConsignmentMetadata" cm JOIN "ConsignmentProperty" cp ON cp."Name" = cm."PropertyName" """ +
      s"""WHERE "ConsignmentId" = ? AND cp."Name" IN ($placeholders);"""

    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    consignmentProperties.zipWithIndex.foreach { case (a, b) =>
      ps.setString(b + 2, a)
    }
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    consignmentProperties.zipWithIndex.foreach({ case (a, b) =>
      rs.getString("PropertyName") should equal(a)
      rs.getString("Value") should equal(expectedPropertyValues(b))
      rs.next()
    })
  }
}
