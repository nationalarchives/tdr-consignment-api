package uk.gov.nationalarchives.tdr.api.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.{Origin, `Strict-Transport-Security`}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.directives.{Credentials, DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, Route, RouteResult}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.typesafe.config._
import com.typesafe.scalalogging.Logger
import spray.json.JsValue
import uk.gov.nationalarchives.tdr.api.auth.AuthorisationException
import uk.gov.nationalarchives.tdr.keycloak.{KeycloakUtils, TdrKeycloakDeployment, Token}

import scala.concurrent.{ExecutionContext, Future}

class Routes(val config: Config) extends Cors {

  private val logger = Logger(classOf[Routes])

  implicit val system: ActorSystem = ActorSystem("consignmentApi")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContext = system.dispatcher
  val url: String = config.getString("auth.url")
  val ttlSeconds: Int = 3600
  val transportSecurityMaxAge = 31536000
  implicit val keycloakDeployment: TdrKeycloakDeployment = TdrKeycloakDeployment(url, "tdr", ttlSeconds)

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

  val route: Route = logging {
    optionalHeaderValueByType[Origin](()) { originHeader =>
      corsHandler((post & path("graphql")) {
        authenticateOAuth2Async("tdr", tokenAuthenticator) { accessToken =>
          respondWithHeader(`Strict-Transport-Security`(transportSecurityMaxAge, includeSubDomains = true)) {
            entity(as[JsValue]) { requestJson =>
              GraphQLServer.endpoint(requestJson, accessToken)
            }
          }
        }
      }, originHeader)
    } ~ (get & path("healthcheck")) {
      complete(StatusCodes.OK)
    }
  }
}
