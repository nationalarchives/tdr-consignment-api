package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.stream.alpakka.slick.javadsl.SlickSession
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.validUserToken
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._

import java.sql.PreparedStatement
import scala.io.Source.fromResource

class ErrorRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  val fields = List("getConsignment", "getSeries", "getTransferAgreement")

  "getConsignment" should "return the field name in the error message" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val query: String = fromResource(s"json/getconsignment_query_alldata.json").mkString
      createDatabaseError(utils)
      val route = new Routes(ConfigFactory.load, SlickSession.forConfig("consignmentapi")).route
      Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
        val entityResponse = entityAs[String]
        response.status should equal(StatusCodes.InternalServerError)
        entityResponse should equal(s"Request with field getConsignment failed")
      }
      resetRenamedColumn(utils)
  }

  private def createDatabaseError(utils: TestUtils): Int = {
    val sql = """ALTER TABLE "Consignment" RENAME COLUMN "ConsignmentReference" TO "ConsignmentReferenceOld";"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.executeUpdate()
  }

  private def resetRenamedColumn(utils: TestUtils): Int = {
    val sql = """ALTER TABLE "Consignment" RENAME COLUMN "ConsignmentReferenceOld" TO "ConsignmentReference"; """
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.executeUpdate()
  }
}
