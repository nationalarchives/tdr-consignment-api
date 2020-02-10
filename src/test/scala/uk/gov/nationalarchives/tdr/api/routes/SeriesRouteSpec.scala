package uk.gov.nationalarchives.tdr.api.routes

import java.sql.PreparedStatement

import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.io.Source.fromResource
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.http.Routes.route
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

class SeriesRouteSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest with BeforeAndAfterEach  {

  override def beforeEach(): Unit = {
    DbConnection.db.source.createConnection().prepareStatement("delete from consignmentapi.Series").executeUpdate()
  }

  val getSeriesQuery: String = fromResource("json/getseries_query_somedata.json").mkString

  "The api" should "return an empty series list" in {
    val expectedResult: String = fromResource("json/getseries_data_empty.json").mkString

    Post("/graphql").withEntity(ContentTypes.`application/json`, getSeriesQuery) ~> addCredentials(validToken) ~> route ~> check {
      responseAs[String] shouldEqual expectedResult
    }
  }

  "The api" should "return the expected data" in {
    val ps: PreparedStatement = DbConnection.db.source.createConnection()
      .prepareStatement("insert into consignmentapi.Series (SeriesId, BodyId) VALUES (1, 1)")
    ps.executeUpdate()

    val query: String = """{"query":"{getSeries(body: \"Body\"){seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
      responseAs[String] shouldEqual """{"data":{"getSeries":[{"seriesid":1}]}}"""
    }
  }

  "The api" should "return all requested fields" in {
    val sql = "insert into consignmentapi.Series (SeriesId, BodyId, Name, Code, Description) VALUES (1,1,'Name','Code','Description')"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.executeUpdate()
    val query: String = """{"query":"{getSeries(body: \"Body\"){seriesid, bodyid, name, code, description}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
      val result = """{"data":{"getSeries":[{"name":"Name","description":"Description","seriesid":1,"code":"Code","bodyid":1}]}}"""
      responseAs[String] shouldEqual result
    }
  }

  "The api" should "return an error if a user queries without a body argument" in {
    val query: String = """{"query":"{getSeries{seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
      val response = "{\"data\":null,\"errors\":[{\"message\":\"Body for user 1 was  in the query and Body in the " +
        "token\",\"path\":[\"getSeries\"],\"locations\":[{\"column\":2,\"line\":1}]}]}"
      responseAs[String] shouldEqual response
    }
  }

  "The api" should "return an error if a user queries with a different body to their own" in {
    val query: String = """{"query":"{getSeries(body: \"Body2\"){seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
      val response = "{\"data\":null,\"errors\":[{\"message\":\"Body for user 1 was Body2 in the query and Body in the " +
        "token\",\"path\":[\"getSeries\"],\"locations\":[{\"column\":2,\"line\":1}]}]}"
      responseAs[String] shouldEqual response
    }
  }

  "The api" should "return an error if a user queries with the correct body but it is not set on their user" in {
    val query: String = """{"query":"{getSeries(body: \"Body2\"){seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserTokenNoBody) ~> route ~> check {
      val response = "{\"data\":null,\"errors\":[{\"message\":\"Body for user 1 was Body2 in the query and null in the " +
        "token\",\"path\":[\"getSeries\"],\"locations\":[{\"column\":2,\"line\":1}]}]}"
      responseAs[String] shouldEqual response
    }
  }

  "The api" should "return all series if an admin user queries without a body argument" in {
    val sql = "insert into consignmentapi.Series (SeriesId, BodyId) VALUES (1,1), (2, 2)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.executeUpdate()
    val query: String = """{"query":"{getSeries{seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validAdminToken) ~> route ~> check {
      val response = """{"data":{"getSeries":[{"seriesid":1},{"seriesid":2}]}}"""
      responseAs[String] shouldEqual response
    }
  }

  "The api" should "return the correct series if an admin queries with a body argument" in {
    val sql = "insert into consignmentapi.Series (SeriesId, BodyId) VALUES (1,1), (2, 2)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.executeUpdate()
    val query: String = """{"query":"{getSeries(body:\"Body\"){seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validAdminToken) ~> route ~> check {
      val response = """{"data":{"getSeries":[{"seriesid":1}]}}"""
      responseAs[String] shouldEqual response
    }
  }

  "The api" should "return all requested fields from inserted Series object" in {

    val mutation: String = fromResource("json/addseries_mutation_alldata.json").mkString
    val expectedResult: String = fromResource("json/addseries_data_all.json").mkString

    Post("/graphql").withEntity(ContentTypes.`application/json`, mutation) ~> addCredentials(validToken) ~> route ~> check {
      responseAs[String] shouldEqual expectedResult
    }
  }

  "The api" should "return the expected data from inserted Series object" in {

    val mutation: String = fromResource("json/addseries_mutation_somedata.json").mkString
    val expectedResult: String = fromResource("json/addseries_data_some.json").mkString

    Post("/graphql").withEntity(ContentTypes.`application/json`, mutation) ~> addCredentials(validToken) ~> route ~> check {
      responseAs[String] shouldEqual expectedResult
    }
  }
}
