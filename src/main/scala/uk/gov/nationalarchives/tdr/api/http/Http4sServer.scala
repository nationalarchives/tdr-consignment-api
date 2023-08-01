package uk.gov.nationalarchives.tdr.api.http

import cats.effect._
import com.comcast.ip4s.IpLiteralSyntax
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.{Json, JsonObject}
import io.circe.generic.auto._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.ember.server._
import org.http4s.headers.{Authorization, Origin, `Content-Type`, `Strict-Transport-Security`}
import org.http4s.implicits._
import org.http4s.server.Server
import org.http4s.server.middleware.CORS
import sangria.parser.QueryParser
import slick.jdbc.JdbcBackend
import uk.gov.nationalarchives.tdr.api.db.DbConnectionHttp4s
import uk.gov.nationalarchives.tdr.api.service.FullHealthCheckService
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

class Http4sServer(database: JdbcBackend#DatabaseDef) {
  case class Query(query: String, operationName: Option[String], variables: Option[Json])
  implicit val decoder: EntityDecoder[IO, Query] = jsonOf[IO, Query]
  val config: Config = ConfigFactory.load
  val url: String = config.getString("auth.url")
  implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(url, "tdr", 3600)
  val graphqlServer: GraphQLServerHttp4s = GraphQLServerHttp4s()
  val transportSecurityMaxAge = 31536000
  val fullHealthCheck = new FullHealthCheckService()

  def jsonApp: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ OPTIONS -> Root / "graphql" =>
      req.headers.get[Origin].map(_ => Ok()).getOrElse(Forbidden())
    case req @ POST -> Root / "graphql" =>
      for {
        query <- req.as[Query]
        auth <- IO.fromOption(req.headers.get[Authorization])(new Exception("No authorisation header provided"))
        resp <- processQuery(query, auth.value.stripPrefix("Bearer "), database)
      } yield resp
        .withHeaders(
          `Strict-Transport-Security`.unsafeFromLong(transportSecurityMaxAge),
          `Content-Type`(MediaType.application.json)
        )
    case _ @GET -> Root / "healthcheck" => Ok()
    case _ @GET -> Root / "healthcheck-full" =>
      IO.fromFuture(IO(fullHealthCheck.checkDbIsUpAndRunning(DbConnectionHttp4s().db)))
        .flatMap(_ => Ok())
  }

  def corsWrapper: Http[IO, IO] = CORS.policy
    .withAllowOriginHost(Set(Origin.Host(Uri.Scheme.http, Uri.RegName("localhost"), Option(9001))))
    .withAllowMethodsIn(Set(Method.GET, Method.POST))
    .withAllowCredentials(false)
    .withMaxAge(1.day)
    .apply(jsonApp)
    .orNotFound

  def processQuery(query: Query, token: String, database: JdbcBackend#DatabaseDef): IO[Response[IO]] = {
    KeycloakUtils().token(token) match {
      case Left(err) => Forbidden(err.getMessage)
      case Right(accessToken) =>
        QueryParser.parse(query.query) match {
          case Success(queryAst) =>
            Ok(graphqlServer.executeGraphQLQuery(queryAst, query.operationName, query.variables.getOrElse(Json.fromJsonObject(JsonObject.empty)), accessToken, database))
          case Failure(error) =>
            BadRequest(error.getMessage)
        }
    }
  }

  def server: Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(corsWrapper)
    .build
}

object Http4sServer {
  def apply(database: JdbcBackend#DatabaseDef) = new Http4sServer(database)
}
