package uk.gov.nationalarchives.tdr.api.http

import scala.language.postfixOps

object ApiServer extends App {

  new AkkaHttpServer()
}
