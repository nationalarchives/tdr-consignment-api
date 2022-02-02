package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.validUserToken
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestRequest}

import java.sql.PreparedStatement
import scala.io.Source.fromResource

class ErrorRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase {
  val fields = List("getConsignment", "getSeries", "getTransferAgreement")

//  "getConsignment" should "return the field name in the error message" in {
//    val query: String = fromResource(s"json/getconsignment_query_alldata.json").mkString
//    createDatabaseError()
//    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(validUserToken()) ~> route ~> check {
//      val entityResponse = entityAs[String]
//      response.status should equal(StatusCodes.InternalServerError)
//      entityResponse should equal(s"Request with field getConsignment failed")
//    }
//    resetRenamedColumn()
//  }

  private def createDatabaseError(): Int = {
    val sql = "ALTER TABLE Consignment RENAME COLUMN ConsignmentReference TO ConsignmentReferenceOld"
    val ps: PreparedStatement = databaseConnection.prepareStatement(sql)
    ps.executeUpdate()
  }

  private def resetRenamedColumn(): Int = {
    val sql = "ALTER TABLE Consignment RENAME COLUMN ConsignmentReferenceOld TO ConsignmentReference"
    val ps: PreparedStatement = databaseConnection.prepareStatement(sql)
    ps.executeUpdate()
  }
}
