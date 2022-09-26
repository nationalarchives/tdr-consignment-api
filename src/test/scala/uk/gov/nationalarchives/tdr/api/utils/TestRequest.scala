package uk.gov.nationalarchives.tdr.api.utils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.text.utf8.encode
import fs2.{Pure, Stream}
import org.http4s.circe.{jsonEncoder, jsonEncoderOf}
import io.circe.Decoder
import org.http4s.Credentials.Token
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EntityEncoder, Headers, MediaType, Method, Request, Response}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIStringSyntax
import slick.jdbc.JdbcBackend.Database
import uk.gov.nationalarchives.tdr.api.http.Http4sServer

import scala.io.Source.fromResource
import scala.reflect.ClassTag

trait TestRequest extends AnyFlatSpec with Matchers {

  def runTestRequest[A](prefix: String)(queryFileName: String, token: String)
                       (implicit decoder: Decoder[A], classTag: ClassTag[A])
  : A = {
    val database = Database.forConfig("consignmentapi.db")
    val query: String = fromResource(prefix + s"$queryFileName.json").mkString
    val body: Stream[Pure, Byte] = encode(Stream(query))
    val credentials = Token(ci"Bearer", token)
    val headers = Headers(
      `Content-Type`(MediaType.application.json),
      Authorization(credentials)
    )
    val response: Response[IO] = Http4sServer(database).corsWrapper.run(
      Request(method = Method.POST, uri = uri"/graphql", body = body, headers = headers)
    ).unsafeRunSync()
    response.as[A].unsafeRunSync()
  }
}
