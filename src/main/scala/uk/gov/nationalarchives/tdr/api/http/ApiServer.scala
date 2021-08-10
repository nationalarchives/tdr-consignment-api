package uk.gov.nationalarchives.tdr.api.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.concurrent.Await
import scala.language.postfixOps

object ApiServer extends App {

  val PORT = 8080
  val logger = Logger("ApiServer")

  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: Materializer = Materializer(actorSystem)

  import scala.concurrent.duration._

  scala.sys.addShutdownHook(() -> shutdown())

  val routes = new Routes(ConfigFactory.load())

  Http().bindAndHandle(routes.route, "0.0.0.0", PORT)
  logger.info(s"Consignment API is running")


  def shutdown(): Unit = {
    actorSystem.terminate()
    Await.result(actorSystem.whenTerminated, 30 seconds)
  }
}
