package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.HttpChallenge
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.AuthenticationFailedRejection
import akka.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalacache.CacheConfig
import scalacache.caffeine.CaffeineCache
import slick.jdbc.JdbcBackend
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.DbConnection.getPassword
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestDatabase
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{addTransferringBody, invalidToken, validUserToken}

import java.sql.PreparedStatement
import java.util.UUID
import scala.util.{Failure, Success}

class RouteAuthenticationSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest with TestDatabase {

  val route = new Routes(ConfigFactory.load(), DbConnection.db).route

  "The api" should "return ok" in {
    Get("/healthcheck") ~> route ~> check {
      responseAs[String] shouldEqual "OK"
    }
  }

  "The api" should "return a rejected credentials error" in {
    Post("/graphql") ~> addCredentials(invalidToken) ~> route ~> check {
      rejection shouldBe AuthenticationFailedRejection(CredentialsRejected, HttpChallenge("Bearer", Some("tdr")))
    }
  }

  "The api" should "return a missing credentials error" in {
    Post("/graphql") ~> route ~> check {
      rejection shouldBe AuthenticationFailedRejection(CredentialsMissing, HttpChallenge("Bearer", Some("tdr")))
    }
  }

  "The api" should "return a valid response with a valid token" in {
    val query: String = """{"query":"{getSeries(body:\"Body\"){seriesid}}"}"""
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  "The db" should "return ok when there is at least one transferring body in the db" in {
    addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")
    Get("/healthcheck-full") ~> route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  "The db" should "return 500 Internal Server Error if there are no transferring bodies in the db" in {
    Get("/healthcheck-full") ~> route ~> check {
      status shouldEqual StatusCodes.InternalServerError
    }
  }

  "The db" should "return 500 Internal Server Error if the db is down" in {
    object DbConnectionTest {
      implicit val passwordCache: CaffeineCache[String] = CaffeineCache[String](CacheConfig())
      val db: JdbcBackend#DatabaseDef = SlickSession.forConfig("consignmentapi").db

      def addTransferringBody(id: UUID, name: String, code: String): Unit = {
        val sql = s"INSERT INTO Body (BodyId, Name, TdrCode) VALUES (?, ?, ?)"
        val ps: PreparedStatement = this.db.source.createConnection().prepareStatement(sql)
        ps.setString(1, id.toString)
        ps.setString(2, name)
        ps.setString(3, code)

        ps.executeUpdate()
      }
    }

    val testDb = DbConnectionTest.db
    /* Add transferring body to ensure that the 'Internal Server Error' produced by this test is due to db being closed
     and not due to db not having any transferring bodies */
    DbConnectionTest.addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")
    val route = new Routes(ConfigFactory.load(), testDb).route
    testDb.close()
    Get("/healthcheck-full") ~> route ~> check {
      status shouldEqual StatusCodes.InternalServerError
    }
  }
}
