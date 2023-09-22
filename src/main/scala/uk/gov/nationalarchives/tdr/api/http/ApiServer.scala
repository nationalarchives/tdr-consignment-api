package uk.gov.nationalarchives.tdr.api.http

import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.postgresql.Driver
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService

import scala.language.postfixOps

object ApiServer extends IOApp {

  val logger: Logger = Logger("ApiServer")
  val config: Config = ConfigFactory.load()
  val blockHttp4s: Boolean = config.getBoolean("featureAccessBlock.http4s")

  override def run(args: List[String]): IO[ExitCode] = {
    if (blockHttp4s) {
      new ReferenceGeneratorService()
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
      val databaseConfig: JdbcBackend#DatabaseDef = DatabaseConfig.forConfig[JdbcProfile]("consignmentapi", config).db

      val server = new Http4sServer(databaseConfig).server
      server.use(_ => IO.never).as(ExitCode.Success)
    }
  }
}
