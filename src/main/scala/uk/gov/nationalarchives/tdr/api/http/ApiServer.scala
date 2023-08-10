package uk.gov.nationalarchives.tdr.api.http

import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.postgresql.Driver
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.language.postfixOps

object ApiServer extends IOApp {

  val logger: Logger = Logger("ApiServer")
  val config: Config = ConfigFactory.load()

  override def run(args: List[String]): IO[ExitCode] = {
    logger.info(s"Consignment API is running using HTTP4S")
    val databaseConfig: JdbcBackend#DatabaseDef = DatabaseConfig.forConfig[JdbcProfile]("consignmentapi", config).db
    val server = new Http4sServer(databaseConfig).server
    server.use(_ => IO.never).as(ExitCode.Success)
  }
}
