package uk.gov.nationalarchives.tdr.api.http

import akka.http.scaladsl.model.HttpMethods.{GET, OPTIONS, POST}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, options, respondWithHeaders, _}
import akka.http.scaladsl.server.{Directive0, Route}
import com.typesafe.config.Config

import scala.jdk.CollectionConverters._

trait Cors {

  def config: Config

  val frontendUrls: Seq[String] = config.getStringList("frontend.urls").asScala.toSeq

  private def addAccessControlHeaders(originHeader: Option[Origin]): Directive0 = {
    respondWithHeaders(
      `Access-Control-Allow-Origin`(HttpOrigin(allowedOrigin(originHeader))),
      `Access-Control-Allow-Credentials`(true),
      `Access-Control-Allow-Headers`("Authorization", "Content-Type", "X-Requested-With"),
      `Access-Control-Allow-Methods`(OPTIONS, POST, GET)
    )
  }

  private def allowedOrigin(originHeader: Option[Origin]): String = {
    val domainsInHeader = originHeader
      .map(_.origins)
      .getOrElse(Seq.empty)
      .map(_.toString)
    val overlappingDomains = domainsInHeader.intersect(frontendUrls)

    val defaultAllowedOrigin = frontendUrls.head
    overlappingDomains.headOption.getOrElse(defaultAllowedOrigin)
  }

  private def preflightRequestHandler: Route = options {
    complete(HttpResponse(StatusCodes.OK))
  }

  def corsHandler(r: Route, originHeader: Option[Origin]): Route = addAccessControlHeaders(originHeader) {
    preflightRequestHandler ~ r
  }
}
