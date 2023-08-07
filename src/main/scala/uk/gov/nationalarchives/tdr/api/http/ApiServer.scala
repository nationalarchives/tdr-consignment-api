package uk.gov.nationalarchives.tdr.api.http

import com.typesafe.config.{Config, ConfigFactory}
import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.tdr.api.db.DbConnectionHttp4s

import scala.language.postfixOps

object ApiServer extends IOApp {

  val logger: Logger = Logger("ApiServer")
  val config: Config = ConfigFactory.load()
  val blockHttp4s: Boolean = config.getBoolean("featureAccessBlock.http4s")

  override protected def blockedThreadDetectionEnabled = true

  override def run(args: List[String]): IO[ExitCode] = {
    if (blockHttp4s) {
      val akkaHttpServer = new AkkaHttpServer()
      val serverBindingFuture = akkaHttpServer.start
      val finalIO = IO
        .fromFuture(IO(serverBindingFuture))
        .flatMap { serverBinding =>
          logger.info(s"Consignment API is running using AKKA")
          IO.never
        }
        .guaranteeCase { exitCase =>
          IO(akkaHttpServer.shutdown())
        }

      finalIO.as(ExitCode.Success)
    } else {
      logger.info(s"Consignment API is running using HTTP4S")
      val server = new Http4sServer(DbConnectionHttp4s().db).server
      server.use(_ => IO.never).as(ExitCode.Success)
    }
  }
}
