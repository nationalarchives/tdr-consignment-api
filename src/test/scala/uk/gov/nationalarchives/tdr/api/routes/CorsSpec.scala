package uk.gov.nationalarchives.tdr.api.routes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.http4s.headers._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Header, Headers, MediaType, Method, Request, Response}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIStringSyntax
import slick.jdbc.JdbcBackend.Database
import uk.gov.nationalarchives.tdr.api.http.Http4sServer

import scala.jdk.CollectionConverters._

class CorsSpec extends AnyFlatSpec with Matchers {

  private val defaultCrossOriginDomain = "https://some-frontend.example.com"
  private val secondaryCrossOriginDomain = "https://other-frontend.example.com"
  private val crossOriginUrls = List(defaultCrossOriginDomain, secondaryCrossOriginDomain).asJava
  val database = Database.forConfig("consignmentapi.db")

  private val config = ConfigFactory.load()
    .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))

  "the pre-flight request" should "allow credentials, required headers and methods" in {
    val headers = Headers(
      `Content-Type`(MediaType.application.json)
    )
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    response.headers.get[`Access-Control-Allow-Credentials`] should contain("true")
    response.headers.get[`Access-Control-Allow-Headers`] should contain("true")
    response.headers.get[`Access-Control-Allow-Methods`] should contain("true")
  }

  "the pre-flight request" should "allow requests from the default cross-origin URL" in {
    val headers = Headers(
      Header.Raw(ci"Origin", defaultCrossOriginDomain)
    )
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(defaultCrossOriginDomain)
  }

  "the pre-flight request" should "allow requests from other configured cross-origin URLs" in {
    val headers = Headers(
      Header.Raw(ci"Origin", secondaryCrossOriginDomain)
    )
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(secondaryCrossOriginDomain)
  }

  "the pre-flight request" should "return the default origin if a different origin is given" in {
    val headers = Headers(
      Header.Raw(ci"Origin", "https://yet-another-domain.example.com")
    )
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(defaultCrossOriginDomain)
  }

  "the pre-flight request" should "return the default origin if no origin is given" in {
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql")
    ).unsafeRunSync()
    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(defaultCrossOriginDomain)
  }

  "the pre-flight request" should "allow requests from an allowed port" in {
    val allowedDomain = "http://localhost:1234"
    val headers = Headers(
      Header.Raw(ci"Origin", allowedDomain)
    )

    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    val crossOriginUrls = List(allowedDomain).asJava

    val testConfig = ConfigFactory.load()
      .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))

    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(allowedDomain)
  }

  "the pre-flight request" should "not allow requests from an allowed domain with a different port" in {
    val allowedDomain = "http://localhost:1234"
    val domainWithOtherPort = "http://localhost:5678"
    val headers = Headers(
      Header.Raw(ci"Origin", domainWithOtherPort)
    )

    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    val crossOriginUrls = List(allowedDomain).asJava

    val testConfig = ConfigFactory.load()
      .withValue("frontend.urls", ConfigValueFactory.fromIterable(crossOriginUrls))

    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(allowedDomain)
  }

  "the pre-flight request" should "return an allowed domain if multiple origins are given" in {
    val headers = Headers(
      Header.Raw(ci"Origin", "https://yet-another-domain.example.com"),
      Header.Raw(ci"Origin", secondaryCrossOriginDomain),
      Header.Raw(ci"Origin", "https://a-third-domain.example.com")
    )

    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", headers = headers)
    ).unsafeRunSync()
    response.headers.headers.filter(_.name.toString == "Access-Control-Allow-Origin").map(_.value) should contain(secondaryCrossOriginDomain)
  }
}
