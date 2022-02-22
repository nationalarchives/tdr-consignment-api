package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FinalTransferConfirmationRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  private val addFinalTransferConfirmationJsonFilePrefix: String = "json/addfinaltransferconfirmation_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddFinalTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class GraphqlQueryData(data: Option[FinalTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class FinalTransferConfirmation(
                                        consignmentId: Option[UUID] = None,
                                        finalOpenRecordsConfirmed: Option[Boolean] = None,
                                        legalOwnershipTransferConfirmed: Option[Boolean] = None
                                      )

  case class AddFinalTransferConfirmation(addFinalTransferConfirmation: FinalTransferConfirmation)

  private val consignmentId = UUID.fromString("b42dccf0-549a-4204-bc9e-c6b69560b7a5")

  val runTestMutation: (String, OAuth2BearerToken, Config) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addFinalTransferConfirmationJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addFinalTransferConfirmationJsonFilePrefix)

  private val addFinalJudgmentTransferConfirmationJsonFilePrefix: String = "json/addfinaljudgmenttransferconfirmation_"

  case class GraphqlJudgmentMutationData(data: Option[AddFinalJudgmentTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class GraphqlJudgmentQueryData(data: Option[FinalJudgmentTransferConfirmation], errors: List[GraphqlError] = Nil)

  case class FinalJudgmentTransferConfirmation(
                                                consignmentId: Option[UUID] = None,
                                                legalCustodyTransferConfirmed: Option[Boolean] = None
                                              )

  case class AddFinalJudgmentTransferConfirmation(addFinalJudgmentTransferConfirmation: FinalJudgmentTransferConfirmation)

  val runTestJudgmentMutation: (String, OAuth2BearerToken, Config) => GraphqlJudgmentMutationData =
    runTestRequest[GraphqlJudgmentMutationData](addFinalJudgmentTransferConfirmationJsonFilePrefix)
  val expectedJudgmentMutationResponse: String => GraphqlJudgmentMutationData =
    getDataFromFile[GraphqlJudgmentMutationData](addFinalJudgmentTransferConfirmationJsonFilePrefix)

  "The api" should "return all requested fields from inserted final transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(consignmentId, userId)
      finalTransferConfirmationProperties.foreach(ftcp => utils.addConsignmentProperty(ftcp))
      val appConfig = config(container)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(), appConfig)
      response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

      checkFinalTransferConfirmationExists(consignmentId, appConfig)
  }

  "The api" should "return all requested fields from inserted final judgment transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(consignmentId, userId)
      finalJudgmentTransferConfirmationProperties.foreach(ftcp => utils.addConsignmentProperty(ftcp))
      val appConfig = config(container)

      val expectedResponse: GraphqlJudgmentMutationData = expectedJudgmentMutationResponse("data_all")
      val response: GraphqlJudgmentMutationData = runTestJudgmentMutation("mutation_alldata", validUserToken(), appConfig)
      response.data.get.addFinalJudgmentTransferConfirmation should equal(expectedResponse.data.get.addFinalJudgmentTransferConfirmation)

      checkFinalJudgmentTransferConfirmationExists(consignmentId, appConfig)
  }

  "The api" should "return the expected data from inserted final transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(consignmentId, userId)
      finalTransferConfirmationProperties.foreach(ftcp => utils.addConsignmentProperty(ftcp))
      val appConfig = config(container)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", validUserToken(), appConfig)
      response.data.get.addFinalTransferConfirmation should equal(expectedResponse.data.get.addFinalTransferConfirmation)

      checkFinalTransferConfirmationExists(response.data.get.addFinalTransferConfirmation.consignmentId.get, appConfig)
  }

  "The api" should "return the expected data from inserted final judgment transfer confirmation consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(consignmentId, userId)
      finalJudgmentTransferConfirmationProperties.foreach(ftcp => utils.addConsignmentProperty(ftcp))
      val appConfig = config(container)

      val expectedResponse: GraphqlJudgmentMutationData = expectedJudgmentMutationResponse("data_some")
      val response: GraphqlJudgmentMutationData = runTestJudgmentMutation("mutation_somedata", validUserToken(), appConfig)
      response.data.get.addFinalJudgmentTransferConfirmation should equal(expectedResponse.data.get.addFinalJudgmentTransferConfirmation)

      checkFinalJudgmentTransferConfirmationExists(response.data.get.addFinalJudgmentTransferConfirmation.consignmentId.get, appConfig)
  }

  "The api" should "throw an error if the consignment id field is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_consignmentid_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingconsignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "throw an error if the consignment id field is not provided for judgment user" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlJudgmentMutationData = expectedJudgmentMutationResponse("data_consignmentid_missing")
      val response: GraphqlJudgmentMutationData = runTestJudgmentMutation("mutation_missingconsignmentid", validUserToken(), config(container))
      print(response.errors.head.message)
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if a user does not own the final transfer confirmation's consignment id" in withContainers {
    case container: PostgreSQLContainer =>
      val userTwoId = UUID.fromString("ef056fd5-22ab-4e01-9e1e-1e65e5907d99")
      databaseUtils(container).createConsignment(consignmentId, userTwoId)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_not_owner")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if a user does not own the final judgment transfer confirmation's consignment id" in withContainers {
    case container: PostgreSQLContainer =>
      val userTwoId = UUID.fromString("ef056fd5-22ab-4e01-9e1e-1e65e5907d99")
      databaseUtils(container).createConsignment(consignmentId, userTwoId)

      val expectedResponse: GraphqlJudgmentMutationData = expectedJudgmentMutationResponse("data_error_not_owner")
      val response: GraphqlJudgmentMutationData = runTestJudgmentMutation("mutation_alldata", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if an invalid consignment id is provided" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_invalid_consignmentid")
      val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if an invalid consignment id is provided for a judgment" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlJudgmentMutationData = expectedJudgmentMutationResponse("data_error_invalid_consignmentid")
      val response: GraphqlJudgmentMutationData = runTestJudgmentMutation("mutation_invalid_consignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def checkFinalTransferConfirmationExists(consignmentId: UUID, config: Config): Unit = {
    val sql =
      """SELECT * FROM "ConsignmentMetadata"
                 WHERE "ConsignmentId" = ? AND "PropertyName" in ('FinalOpenRecordsConfirmed', 'LegalOwnershipTransferConfirmed');"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("Value") should equal("true")
    rs.next()
    rs.getString("Value") should equal("true")
  }

  private def checkFinalJudgmentTransferConfirmationExists(consignmentId: UUID, config: Config): Unit = {
    val sql =
      """SELECT * FROM "ConsignmentMetadata"
                 WHERE "ConsignmentId" = ? AND "PropertyName" in ('LegalCustodyTransferConfirmed');"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("Value") should equal("true")
  }
}
