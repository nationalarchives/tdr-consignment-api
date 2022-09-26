package uk.gov.nationalarchives.tdr.api.http

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.tdr.api.db.DbConnection

import scala.language.postfixOps

object ApiServer extends IOApp {

  val logger = Logger("ApiServer")
  val server = new Http4sServer(DbConnection().db).server

  override def run(args: List[String]): IO[ExitCode] = server.use(_ => IO.never).as(ExitCode.Success)
}
