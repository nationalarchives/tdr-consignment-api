package uk.gov.nationalarchives.tdr.api.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.headers.{Origin, `Strict-Transport-Security`}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.directives.{Credentials, DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, ExceptionHandler, Route, RouteResult}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.typesafe.config._
import com.typesafe.scalalogging.Logger
import sangria.ast.{Field, OperationDefinition}
import sangria.parser.QueryParser
import spray.json.{JsObject, JsString, JsValue}
import uk.gov.nationalarchives.tdr.api.auth.AuthorisationException
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FullHealthCheckService
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment, Token}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

class Routes(val config: Config) extends Cors {
  val url: String = config.getString("auth.url")

  implicit val system: ActorSystem = ActorSystem("consignmentApi")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContext = system.dispatcher
  val ttlSeconds: Int = 3600
  val transportSecurityMaxAge = 31536000
  val exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: Throwable =>
        entity(as[JsValue]) { requestJson =>
          val JsObject(fields) = requestJson
          val JsString(query) = fields("query")

          QueryParser.parse(query) match {
            case Failure(parseException) =>
              logger.error(s"Error parsing entity $requestJson", parseException)
              logger.error(ex.getMessage, ex)
              complete(HttpResponse(InternalServerError, entity = ex.getMessage))
            case Success(document) =>
              val fieldName = document.definitions.headOption.flatMap {
                case operationDefinition: OperationDefinition => operationDefinition.selections.headOption.map {
                  case field: Field => field.outputName
                  case _ => None
                }
                case _ => None
              }.getOrElse("Unknown field")

              val errorMessage = s"Request with field $fieldName failed ${ex.getMessage}"
              logger.error(errorMessage, ex)
              complete(HttpResponse(InternalServerError, entity = errorMessage))
          }
        }
    }
  implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(url, "tdr", ttlSeconds)
  val route: Route =
    logging {
      handleExceptions(exceptionHandler) {
        optionalHeaderValueByType(Origin) { originHeader =>
          corsHandler((post & path("graphql")) {
            authenticateOAuth2Async("tdr", tokenAuthenticator) { accessToken =>
              respondWithHeader(`Strict-Transport-Security`(transportSecurityMaxAge, includeSubDomains = true)) {
                entity(as[JsValue]) { requestJson =>
                  GraphQLServer.endpoint(requestJson, accessToken, config)
                }
              }
            }
          }, originHeader)
        }
      } ~ (get & path("healthcheck")) {
        complete(StatusCodes.OK)
      } ~ (get & path("healthcheck-full")) {
        val fullHealthCheck = new FullHealthCheckService()
        onSuccess(fullHealthCheck.checkDbIsUpAndRunning(DbConnection.db)) {
          complete(StatusCodes.OK)
        }
      }
    }

  private val logger = Logger(classOf[Routes])

  // We return None rather than a failed future because we're following the async authenticator docs
  // https://doc.akka.io/docs/akka-http/10.0/routing-dsl/directives/security-directives/authenticateOAuth2Async.html
  def tokenAuthenticator(credentials: Credentials): Future[Option[Token]] = {
    credentials match {
      case Credentials.Provided(token) => Future {
        KeycloakUtils().token(token).left.map(
          e => {
            logger.error(e.getMessage, e)
            AuthorisationException(e.getMessage)
          }).toOption
      }
      case _ => Future.successful(None)
    }
  }

  def logging: Directive0 = {
    def logNon200Response(req: HttpRequest)(res: RouteResult): Unit = {
      res match {
        case Complete(resp) =>
          if (resp.status.isFailure()) {
            logger.warn(s"Non 200 Response - Request: $req, Request Entity: ${Unmarshal(req.entity).to[String].toString}, " +
              s"Response: ${Unmarshal(resp).to[String].toString}")
          }
        case Rejected(reason) =>
          logger.warn(s"Rejected Reason: " + reason.mkString(", "))
      }
    }

    DebuggingDirectives.logRequestResult(LoggingMagnet(_ => logNon200Response))
  }
}
