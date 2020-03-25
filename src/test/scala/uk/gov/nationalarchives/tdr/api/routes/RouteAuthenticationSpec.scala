package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.HttpChallenge
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.AuthenticationFailedRejection
import akka.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import uk.gov.nationalarchives.tdr.api.http.Routes.route
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{validUserToken,invalidToken}

class RouteAuthenticationSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {

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
}
