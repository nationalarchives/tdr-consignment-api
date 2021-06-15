package uk.gov.nationalarchives.tdr.api.routes

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestRequest, TestUtils}

class SeriesRouteSpec extends AnyFlatSpec with Matchers with TestDatabase with TestRequest {

  private val getSeriesJsonFilePrefix: String = "json/getseries_"
  private val addSeriesJsonFilePrefix: String = "json/addseries_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[GetSeries], errors: List[GraphqlError] = Nil)
  case class GraphqlMutationData(data: Option[AddSeries], errors: List[GraphqlError] = Nil)
  case class Series(
                     bodyid: Option[UUID],
                     seriesid: Option[UUID],
                     name: Option[String] = None,
                     code: Option[String] = None,
                     description: Option[String] = None
                   )
  case class GetSeries(getSeries: List[Series])
  case class AddSeries(addSeries: Series)

  private val bodyCode = "body-code-abcde"

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getSeriesJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addSeriesJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getSeriesJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addSeriesJsonFilePrefix)

  "The api" should "return an empty series list if the body has no series" in {
    val bodyId = UUID.fromString("90cb9602-9794-4945-bedf-f01632b266c3")
    TestUtils.addTransferringBody(bodyId, "Some body name", bodyCode)

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_empty")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken(body = bodyCode))
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return all series belonging to the user's transferring body" in {
    val bodyId = UUID.fromString("260f90b1-9648-46c0-b8c5-e5a725fbc667")
    val otherBodyId = UUID.fromString("534845ee-dd2a-4566-a348-d91e4a74a998")

    TestUtils.addTransferringBody(bodyId, "Some body name", bodyCode)
    TestUtils.addTransferringBody(otherBodyId, "Some body name", "other-body-code")
    TestUtils.addSeries(UUID.fromString("d737dc4a-cd9b-4ac3-8b33-ab30ee8d3241"), bodyId, "series-code-1")
    TestUtils.addSeries(UUID.fromString("769d319f-4faa-4ab2-ab52-46bc7e6e1e3d"), bodyId, "series-code-2")
    TestUtils.addSeries(UUID.fromString("01d2eb57-9d35-43e8-9eff-63e539ada1f9"), otherBodyId, "series-code-3")

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken(body = bodyCode))
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return all requested fields" in {
    val bodyId = UUID.fromString("283b28bf-a0cc-475a-9ce9-1ba60631afa7")

    val bodyCode = "body-code-xyz"
    TestUtils.addSeries(
      UUID.fromString("a9f96d9b-ca53-41c3-937d-88c5cb1241da"),
      bodyId,
      "some-series-code"
    )
    TestUtils.addTransferringBody(bodyId, "Some body name", bodyCode)

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(body = bodyCode))
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return an error if a user queries with a different body to their own" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_incorrect_body")
    val response: GraphqlQueryData = runTestQuery("query_incorrect_body", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if a user queries with the correct body but it is not set on their user" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_error_incorrect_user")
    val response: GraphqlQueryData = runTestQuery("query_incorrect_body", validUserTokenNoBody)
    response.data should equal(expectedResponse.data)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }
}
