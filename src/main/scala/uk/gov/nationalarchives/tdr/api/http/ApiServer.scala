package uk.gov.nationalarchives.tdr.api.http

import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.postgresql.Driver
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.language.postfixOps

object ApiServer extends IOApp {

  val logger: Logger = Logger("ApiServer")
  val config: Config = ConfigFactory.load()

  override def run(args: List[String]): IO[ExitCode] = {
      logger.info(s"Consignment API is running using HTTP4S")
      val dbConfig = config.getConfig("consignmentapi.db")
      val postgresDriver = new Driver()
      val dataSource = HikariCPJdbcDataSource.forConfig(dbConfig, postgresDriver, "consignmentApi", ClassLoader.getSystemClassLoader)
      val server = new Http4sServer(dataSource).server
      server.use(_ => IO.never).as(ExitCode.Success)
  }
}
