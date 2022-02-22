package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.validUserToken
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest}

import scala.io.Source.fromResource

class ErrorRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)
  val fields = List("getConsignment", "getSeries", "getTransferAgreement")

  "getConsignment" should "return the field name in the error message" in withContainers {
    case container: PostgreSQLContainer =>
      val route: Route = new Routes(config(container)).route
      val query: String = fromResource(s"json/getconsignment_query_alldata.json").mkString
      databaseUtils(container).createDatabaseError()

      Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
        val entityResponse = entityAs[String]
        response.status should equal(StatusCodes.InternalServerError)
        entityResponse.contains("""Request with field getConsignment failed ERROR: column "ConsignmentReference" does not exist""") should be(true)
      }
  }
}
