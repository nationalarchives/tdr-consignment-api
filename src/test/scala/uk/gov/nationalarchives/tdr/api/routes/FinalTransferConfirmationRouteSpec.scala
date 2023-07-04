package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._

class FinalTransferConfirmationRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFinalTransferConfirmationJsonFilePrefix: String = "json/addfinaltransferconfirmation_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddFinalTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class GraphqlQueryData(data: Option[FinalTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class FinalTransferConfirmation(
      consignmentId: Option[UUID] = None,
      legalCustodyTransferConfirmed: Option[Boolean] = None
  )
  case class AddFinalTransferConfirmation(addFinalTransferConfirmation: FinalTransferConfirmation)

  private val consignmentId = UUID.fromString("b42dccf0-549a-4204-bc9e-c6b69560b7a5")

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addFinalTransferConfirmationJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addFinalTransferConfirmationJsonFilePrefix)

  "The api" should "return all requested fields from inserted final transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)

      utils.createConsignment(consignmentId, userId)
      (finalJudgmentTransferConfirmationProperties ++ finalTransferConfirmationProperties).foreach(utils.addConsignmentProperty)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
      response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

      checkFinalTransferConfirmationExists(consignmentId, utils)
  }

  "The api" should "return all requested fields from inserted final judgment transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(consignmentId, userId)
      (finalJudgmentTransferConfirmationProperties ++ finalTransferConfirmationProperties).foreach(utils.addConsignmentProperty)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
      response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

      checkFinalJudgmentTransferConfirmationExists(consignmentId, utils)
  }

  "The api" should "return the expected data from inserted final transfer confirmation consignment metadata properties" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    (finalJudgmentTransferConfirmationProperties ++ finalTransferConfirmationProperties).foreach(utils.addConsignmentProperty)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
    val response: GraphqlMutationData = runTestMutation("mutation_somedata", validUserToken())
    response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

    checkFinalTransferConfirmationExists(response.data.get.addFinalTransferConfirmation.consignmentId.get, utils)
  }

  "The api" should "return the expected data from inserted final judgment transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(consignmentId, userId)
      (finalJudgmentTransferConfirmationProperties ++ finalTransferConfirmationProperties).foreach(utils.addConsignmentProperty)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", validUserToken())
      response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

      checkFinalJudgmentTransferConfirmationExists(response.data.get.addFinalTransferConfirmation.consignmentId.get, utils)
  }

  "The api" should "throw an error if the consignment id field is not provided" in withContainers { case _: PostgreSQLContainer =>
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_consignmentid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "throw an error if the consignment id field is not provided for judgment user" in withContainers { case _: PostgreSQLContainer =>
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_consignmentid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if a user does not own the final transfer confirmation's consignment id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val userTwoId = UUID.fromString("ef056fd5-22ab-4e01-9e1e-1e65e5907d99")
    utils.createConsignment(consignmentId, userTwoId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_not_owner")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if a user does not own the final judgment transfer confirmation's consignment id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val userTwoId = UUID.fromString("ef056fd5-22ab-4e01-9e1e-1e65e5907d99")
    utils.createConsignment(consignmentId, userTwoId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_not_owner")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if an invalid consignment id is provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_invalid_consignmentid")
    val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if an invalid consignment id is provided for a judgment" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_invalid_consignmentid")
    val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignmentid", validUserToken())
    println(s"exp : $expectedResponse")
    println(s"act : $response")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def checkFinalTransferConfirmationExists(consignmentId: UUID, utils: TestUtils): Unit = {
    val sql =
      """SELECT * FROM "ConsignmentMetadata"
                 WHERE "ConsignmentId" = ? AND "PropertyName" in ('LegalCustodyTransferConfirmed');"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("Value") should equal("true")
  }

  private def checkFinalJudgmentTransferConfirmationExists(consignmentId: UUID, utils: TestUtils): Unit = {
    val sql =
      """SELECT * FROM "ConsignmentMetadata"
                 WHERE "ConsignmentId" = ? AND "PropertyName" in ('LegalCustodyTransferConfirmed');"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("Value") should equal("true")
  }
}
