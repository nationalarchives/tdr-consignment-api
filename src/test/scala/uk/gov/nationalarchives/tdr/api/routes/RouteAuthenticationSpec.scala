package uk.gov.nationalarchives.tdr.api.routes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.ConfigFactory
import fs2.{Pure, Stream}
import fs2.text.utf8.encode
import org.http4s
import org.http4s.Credentials.Token
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Headers, MediaType, Method, Request, Response, Status}
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIStringSyntax
import uk.gov.nationalarchives.tdr.api.http.Http4sServer
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import scala.io.Source.fromResource

class RouteAuthenticationSpec extends TestContainerUtils with Matchers {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "The api" should "return ok" in withContainers {
    case container: PostgreSQLContainer =>
      val response: Response[IO] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.GET, uri = uri"/healthcheck")
      ).unsafeRunSync()
      response.status should equal(org.http4s.Status.Ok)
  }

  "The api" should "return a rejected credentials error" in withContainers {
    case container: PostgreSQLContainer =>
      val headers = Headers(
        `Content-Type`(MediaType.application.json)
      )
      val response: Either[Throwable, Response[IO]] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.POST, uri = uri"/graphql", headers = headers)
      ).attempt.leftSide.unsafeRunSync()
      response.left.getOrElse(new Exception("")).getMessage shouldBe("failed")
  }

  "The api" should "return a missing credentials error" in withContainers {
    case container: PostgreSQLContainer =>
      val query: String = fromResource(s"json/getconsignment_query_alldata.json").mkString
      val body: Stream[Pure, Byte] = encode(Stream(query))
      val credentials = Token(ci"Bearer", validUserToken())
      val headers = Headers(
        `Content-Type`(MediaType.application.json),
        Authorization(credentials)
      )
      val response: Either[Throwable, Response[IO]] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.POST, uri = uri"/graphql", headers = headers, body = body)
      ).attempt.leftSide.unsafeRunSync()
      response.left.getOrElse(new Exception("")).getMessage shouldBe("failed")
  }

  "The api" should "return a valid response with a valid token" in withContainers {
    case container: PostgreSQLContainer =>
      val query: String = """{"query":"{getSeries(body:\"Body\"){seriesid}}"}"""
      val body: Stream[Pure, Byte] = encode(Stream(query))
      val credentials = Token(ci"Bearer", validUserToken())
      val headers = Headers(
        `Content-Type`(MediaType.application.json),
        Authorization(credentials)
      )
      val response: Response[IO] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.POST, uri = uri"/graphql", headers = headers, body = body)
      ).unsafeRunSync()
      response.status should equal(Status.Ok)
  }

  "The db" should "return ok when there is at least one transferring body in the db" in withContainers {
    case container: PostgreSQLContainer =>
      val response: Response[IO] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.GET, uri = uri"/healthcheck")
      ).unsafeRunSync()
      response.status should equal(org.http4s.Status.Ok)
  }

  "The db" should "return 500 Internal Server Error if there are no transferring bodies in the db" in withContainers {
    case container: PostgreSQLContainer =>
      TestUtils(container.database).deleteSeriesAndBody()
      val response: Response[IO] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.GET, uri = uri"/healthcheck")
      ).unsafeRunSync()
      response.status should equal(org.http4s.Status.InternalServerError)
  }
}
