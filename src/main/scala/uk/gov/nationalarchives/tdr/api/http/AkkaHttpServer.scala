package uk.gov.nationalarchives.tdr.api.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.concurrent.Await
import scala.language.postfixOps

class AkkaHttpServer {
  val port = 8080
  val logger: Logger = Logger("AkkaHttpServer")

  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: Materializer = Materializer(actorSystem)

  import scala.concurrent.duration._

  scala.sys.addShutdownHook(() -> shutdown())

  val slickSession: SlickSession = SlickSession.forConfig("consignmentapi")

  val routes = new Routes(ConfigFactory.load(), slickSession)

  Http().newServerAt("0.0.0.0", port).bindFlow(routes.route)
  logger.info(s"Consignment API is running")

  def shutdown(): Unit = {
    actorSystem.terminate()
    Await.result(actorSystem.whenTerminated, 30 seconds)
  }
}
