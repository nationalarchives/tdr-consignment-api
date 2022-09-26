package uk.gov.nationalarchives.tdr.api.routes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.ConfigFactory
import fs2.text.utf8.encode
import fs2.{Pure, Stream}
import org.http4s.Credentials.Token
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Headers, MediaType, Method, Request, Response}
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIStringSyntax
import uk.gov.nationalarchives.tdr.api.http.Http4sServer
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.validUserToken
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.PreparedStatement
import scala.io.Source.fromResource

class ErrorRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  val fields = List("getConsignment", "getSeries", "getTransferAgreement")

  "getConsignment" should "return the field name in the error message" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      createDatabaseError(utils)
      val query: String = fromResource(s"json/getconsignment_query_alldata.json").mkString
      val body: Stream[Pure, Byte] = encode(Stream(query))
      val credentials = Token(ci"Bearer", validUserToken())
      val headers = Headers(
          `Content-Type`(MediaType.application.json),
          Authorization(credentials)
      )
      val response: Response[IO] = Http4sServer(container.database).corsWrapper.run(
        Request(method = Method.POST, uri = uri"/graphql", headers = headers, body = body)
      ).unsafeRunSync()

      val entityResponse = response.as[String]
      response.status should equal(500)
      entityResponse should equal(s"Request with field getConsignment failed")
      resetRenamedColumn(utils)
  }

  private def createDatabaseError(utils: TestUtils): Int = {
    val sql = """ALTER TABLE "Consignment" RENAME COLUMN "ConsignmentReference" TO "ConsignmentReferenceOld";"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.executeUpdate()
  }

  private def resetRenamedColumn(utils: TestUtils): Int = {
    val sql = """ALTER TABLE "Consignment" RENAME COLUMN "ConsignmentReferenceOld" TO "ConsignmentReference"; """
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.executeUpdate()
  }
}
