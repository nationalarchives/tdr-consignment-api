package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.HttpMethods.{GET, OPTIONS, POST}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.http.Routes

import scala.jdk.CollectionConverters._

class CorsSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {

  private val defaultCrossOriginDomain = "https://some-frontend.example.com"
  private val secondaryCrossOriginDomain = "https://other-frontend.example.com"
  private val crossOriginUrls = List(defaultCrossOriginDomain, secondaryCrossOriginDomain).asJava

  private val config = ConfigFactory.load()
    .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))
  private val route = new Routes(config).route

  "the pre-flight request" should "allow credentials, required headers and methods" in {
    Options("/graphql") ~> route ~> check {
      header[`Access-Control-Allow-Credentials`].map(_.value) should contain("true")
      header[`Access-Control-Allow-Headers`] should contain(`Access-Control-Allow-Headers`("Authorization", "Content-Type", "X-Requested-With"))
      header[`Access-Control-Allow-Methods`] should contain(`Access-Control-Allow-Methods`(OPTIONS, POST, GET))
    }
  }

  "the pre-flight request" should "allow requests from the default cross-origin URL" in {
    val headers = List(Origin(defaultCrossOriginDomain))

    Options("/graphql").withHeaders(headers) ~> route ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(defaultCrossOriginDomain)
    }
  }

  "the pre-flight request" should "allow requests from other configured cross-origin URLs" in {
    val headers = List(Origin(secondaryCrossOriginDomain))

    Options("/graphql").withHeaders(headers) ~> route ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(secondaryCrossOriginDomain )
    }
  }

  "the pre-flight request" should "return the default origin if a different origin is given" in {
    val headers = List(Origin("https://yet-another-domain.example.com"))

    Options("/graphql").withHeaders(headers) ~> route ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(defaultCrossOriginDomain)
    }
  }

  "the pre-flight request" should "return the default origin if no origin is given" in {
    Options("/graphql") ~> route ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(defaultCrossOriginDomain)
    }
  }

  "the pre-flight request" should "allow requests from an allowed port" in {
    val allowedDomain = "http://localhost:1234"
    val crossOriginUrls = List(allowedDomain).asJava

    val testConfig = ConfigFactory.load()
      .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))
    val testRoute = new Routes(testConfig).route

    val headers = List(Origin(allowedDomain))

    Options("/graphql").withHeaders(headers) ~> testRoute ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(allowedDomain)
    }
  }

  "the pre-flight request" should "not allow requests from an allowed domain with a different port" in {
    val allowedDomain = "http://localhost:1234"
    val domainWithOtherPort = "http://localhost:5678"
    val crossOriginUrls = List(allowedDomain).asJava

    val testConfig = ConfigFactory.load()
      .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))
    val testRoute = new Routes(testConfig).route

    val headers = List(Origin(domainWithOtherPort))

    Options("/graphql").withHeaders(headers) ~> testRoute ~> check {
      // Check that the response contains the only allowed port, NOT the requested port
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(allowedDomain)
    }
  }

  "the pre-flight request" should "return an allowed domain if multiple origins are given" in {
    val headers = List(Origin(
      "https://yet-another-domain.example.com",
      secondaryCrossOriginDomain,
      "https://a-third-domain.example.com"
    ))

    Options("/graphql").withHeaders(headers) ~> route ~> check {
      header[`Access-Control-Allow-Origin`].map(_.value) should contain(secondaryCrossOriginDomain)
    }
  }
}
