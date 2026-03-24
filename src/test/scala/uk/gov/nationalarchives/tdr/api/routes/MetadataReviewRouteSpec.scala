package uk.gov.nationalarchives.tdr.api.routes

import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.MetadataReviewFields.MetadataReviewLog
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{GraphqlError, getDataFromFile}
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.time.ZonedDateTime
import java.util.UUID

class MetadataReviewRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val getMetadataReviewDetailsJsonFilePrefix: String = "json/getmetadatareviewdetails_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val consignmentId: UUID = UUID.fromString("3d6c2bfd-6d73-4c20-b45e-58e0685e6ea9")
  val logId: UUID = UUID.fromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

  case class GetMetadataReviewDetails(getMetadataReviewDetails: List[MetadataReviewLog])
  case class GraphqlQueryData(data: Option[GetMetadataReviewDetails], errors: List[GraphqlError] = Nil)

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData =
    runTestRequest[GraphqlQueryData](getMetadataReviewDetailsJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData =
    getDataFromFile[GraphqlQueryData](getMetadataReviewDetailsJsonFilePrefix)

  "getMetadataReviewDetails" should "return all metadata review log entries for the consignment" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    utils.addMetadataReviewLog(logId, consignmentId, userId, "Approve")

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())

    response.data.get.getMetadataReviewDetails.size should equal(1)
    val entry = response.data.get.getMetadataReviewDetails.head
    entry.metadataReviewLogId should equal(expectedResponse.data.get.getMetadataReviewDetails.head.metadataReviewLogId)
    entry.consignmentId should equal(consignmentId)
    entry.userId should equal(userId)
    entry.action should equal("Approve")
    entry.eventTime shouldBe a[ZonedDateTime]
  }

  "getMetadataReviewDetails" should "return an empty list if no log entries exist for the consignment" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_empty")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())

    response.data.get.getMetadataReviewDetails should be(empty)
    response.data should equal(expectedResponse.data)
  }

  "getMetadataReviewDetails" should "return an error if a user does not own the consignment" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
    utils.createConsignment(consignmentId, otherUserId)

    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())

    response.errors should not be empty
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }
}
