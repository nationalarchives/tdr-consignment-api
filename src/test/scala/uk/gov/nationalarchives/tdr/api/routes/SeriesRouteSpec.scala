package uk.gov.nationalarchives.tdr.api.routes

import java.sql.PreparedStatement

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestRequest
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._




class SeriesRouteSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestRequest {

  private val getSeriesJsonFilePrefix: String = "json/getseries_"
  private val addSeriesJsonFilePrefix: String = "json/addseries_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[GetSeries], errors: List[GraphqlError] = Nil)
  case class GraphqlMutationData(data: Option[AddSeries], errors: List[GraphqlError] = Nil)
  case class Series(seriesid: Option[Long], bodyid: Option[Long], name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)
  case class GetSeries(getSeries: List[Series]) extends TestRequest
  case class AddSeries(addSeries: Series)

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getSeriesJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addSeriesJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getSeriesJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addSeriesJsonFilePrefix)

  override def beforeEach(): Unit = {
    DbConnection.db.source.createConnection().prepareStatement("delete from consignmentapi.Series").executeUpdate()
  }


  "The api" should "return an empty series list" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_empty")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return the expected data" in {
    val ps: PreparedStatement = DbConnection.db.source.createConnection()
      .prepareStatement("insert into consignmentapi.Series (SeriesId, BodyId) VALUES (1, 1)")
    ps.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return all requested fields" in {
    val sql = "insert into consignmentapi.Series (SeriesId, BodyId, Name, Code, Description) VALUES (1,1,'Name','Code','Description')"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return an error if a user queries with a different body to their own" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_incorrect_body")
    val response: GraphqlQueryData = runTestQuery("query_incorrect_body", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if a user queries with the correct body but it is not set on their user" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_error_incorrect_user")
    val response: GraphqlQueryData = runTestQuery("query_incorrect_body", validUserTokenNoBody)
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return the correct series if an admin queries with a body argument" in {
    val sql = "insert into consignmentapi.Series (SeriesId, BodyId) VALUES (1,1), (2, 2)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return all requested fields from inserted Series object" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validAdminToken)
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return the expected data from inserted Series object" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
    val response: GraphqlMutationData = runTestMutation("mutation_somedata", validAdminToken)
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return an error if a user has role_user and inserts a Series" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_incorrect_role")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.data should equal(expectedResponse.data)
  }
}
