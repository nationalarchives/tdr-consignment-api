package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.{HttpProtocol, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.server.AuthorizationFailedRejection
import akka.http.scaladsl.server.Directives._
import com.typesafe.config.ConfigFactory
import nl.altindag.log.LogCaptor
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestRequest

import scala.jdk.CollectionConverters._

class RoutesSpec extends AnyFlatSpec with BeforeAndAfterEach with TestRequest {

  private val logCaptor = LogCaptor.forClass(classOf[Routes])
  private val routes = new Routes(ConfigFactory.load())

  override def beforeEach(): Unit = {
    logCaptor.clearLogs()
  }

  "200 response" should "not be logged as warning" in {
    val resp: HttpResponse = createHttpResponse(StatusCodes.OK)

    Post("/") ~> routes.logging(complete(resp))

    logCaptor.getWarnLogs.isEmpty shouldBe(true)
  }

  "non 200 response" should "be logged as warning" in {
    val resp: HttpResponse = createHttpResponse(StatusCodes.BadRequest)

    Post("/") ~> routes.logging(complete(resp))

    val warnings = logCaptor.getWarnLogs.asScala.toList
    warnings.size shouldBe(1)
    warnings.head.contains("Non 200 Response") shouldBe(true)
    warnings.head.contains("Request:") shouldBe(true)
    warnings.head.contains("Response:") shouldBe(true)
  }

  "rejected request" should "be logged as warning" in {
    Post("/") ~> routes.logging(reject(AuthorizationFailedRejection))

    val warnings = logCaptor.getWarnLogs.asScala.toList
    warnings.size shouldBe(1)
    warnings.head shouldEqual("Rejected Reason: AuthorizationFailedRejection")
  }

  private def createHttpResponse(status: StatusCode): HttpResponse = {
    new HttpResponse(status, Seq(), Map(), "entity", HttpProtocol("Http/1.0"))
  }
}
