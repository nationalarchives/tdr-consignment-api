package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService.transferAgreementProperties
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class TransferAgreementRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  private val addTransferAgreementPrivateBetaJsonFilePrefix: String = "json/addtransferagreementprivatebeta_"
  private val addTransferAgreementComplianceJsonFilePrefix: String = "json/addtransferagreementcompliance_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlTAPrivateBetaMutationData(data: Option[AddTransferAgreementPrivateBeta], errors: List[GraphqlError] = Nil)

  case class GraphqlTAComplianceMutationData(data: Option[AddTransferAgreementCompliance], errors: List[GraphqlError] = Nil)

  case class TransferAgreementPrivateBeta(
                                           consignmentId: Option[UUID] = None,
                                           allPublicRecords: Option[Boolean] = None,
                                           allCrownCopyright: Option[Boolean] = None,
                                           allEnglish: Option[Boolean] = None
                                         )

  case class TransferAgreementCompliance(
                                          consignmentId: Option[UUID] = None,
                                          appraisalSelectionSignedOff: Option[Boolean] = None,
                                          initialOpenRecords: Option[Boolean] = None,
                                          sensitivityReviewSignedOff: Option[Boolean] = None)

  case class AddTransferAgreementPrivateBeta(addTransferAgreementPrivateBeta: TransferAgreementPrivateBeta) extends TestRequest

  case class AddTransferAgreementCompliance(addTransferAgreementCompliance: TransferAgreementCompliance) extends TestRequest

  val runTAPrivateBetaTestMutation: (String, OAuth2BearerToken, Config) => GraphqlTAPrivateBetaMutationData =
    runTestRequest[GraphqlTAPrivateBetaMutationData](addTransferAgreementPrivateBetaJsonFilePrefix)
  val expectedTAPrivateBetaMutationResponse: String => GraphqlTAPrivateBetaMutationData =
    getDataFromFile[GraphqlTAPrivateBetaMutationData](addTransferAgreementPrivateBetaJsonFilePrefix)

  val runTAComplianceTestMutation: (String, OAuth2BearerToken, Config) => GraphqlTAComplianceMutationData =
    runTestRequest[GraphqlTAComplianceMutationData](addTransferAgreementComplianceJsonFilePrefix)
  val expectedTAComplianceMutationResponse: String => GraphqlTAComplianceMutationData =
    getDataFromFile[GraphqlTAComplianceMutationData](addTransferAgreementComplianceJsonFilePrefix)


  "addTransferAgreementPrivateBeta" should "return all requested fields from inserted transfer agreement consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.seedDatabaseWithDefaultEntries()
      transferAgreementProperties.foreach(tap => utils.addConsignmentProperty(tap))
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, userId)
      val appConfig = config(container)

      val expectedResponse: GraphqlTAPrivateBetaMutationData = expectedTAPrivateBetaMutationResponse("data_all")
      val response: GraphqlTAPrivateBetaMutationData = runTAPrivateBetaTestMutation("mutation_alldata", validUserToken(), appConfig)

      response.data.get.addTransferAgreementPrivateBeta should equal(expectedResponse.data.get.addTransferAgreementPrivateBeta)

      checkTransferAgreementExists(consignmentId, appConfig)
  }

  "addTransferAgreementPrivateBeta" should "return the expected data from inserted transfer agreement consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      val appConfig = config(container)
      utils.createConsignment(consignmentId, userId)
      transferAgreementProperties.foreach(tap => utils.addConsignmentProperty(tap))

      val expectedResponse: GraphqlTAPrivateBetaMutationData = expectedTAPrivateBetaMutationResponse("data_some")
      val response: GraphqlTAPrivateBetaMutationData = runTAPrivateBetaTestMutation("mutation_somedata", validUserToken(), appConfig)

      response.data.get.addTransferAgreementPrivateBeta should equal(expectedResponse.data.get.addTransferAgreementPrivateBeta)

      checkTransferAgreementExists(consignmentId, appConfig)
  }

  "addTransferAgreementPrivateBeta" should "throw an error if the consignment id field is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlTAPrivateBetaMutationData = expectedTAPrivateBetaMutationResponse("data_consignmentid_missing")
      val response: GraphqlTAPrivateBetaMutationData = runTAPrivateBetaTestMutation("mutation_missingconsignmentid", validUserToken(), config(container))
      print(response, "\n\n\n")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addTransferAgreementPrivateBeta" should "return an error if a user does not own the transfer agreement's consignment id" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val fixedUUIDSource = new FixedUUIDSource()
      val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, otherUserId)

      val expectedResponse: GraphqlTAPrivateBetaMutationData = expectedTAPrivateBetaMutationResponse("data_error_not_owner")
      val response: GraphqlTAPrivateBetaMutationData = runTAPrivateBetaTestMutation("mutation_alldata", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "addTransferAgreementPrivateBeta" should "return an error if an invalid consignment id is provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlTAPrivateBetaMutationData = expectedTAPrivateBetaMutationResponse("data_error_invalid_consignmentid")
      val response: GraphqlTAPrivateBetaMutationData = runTAPrivateBetaTestMutation("mutation_invalid_consignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addTransferAgreementCompliance" should "return all requested fields from inserted transfer agreement consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.seedDatabaseWithDefaultEntries()
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, userId)
      transferAgreementProperties.foreach(tap => utils.addConsignmentProperty(tap))
      val appConfig = config(container)

      val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_all")
      val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_alldata", validUserToken(), appConfig)

      response.data.get.addTransferAgreementCompliance should equal(expectedResponse.data.get.addTransferAgreementCompliance)

      checkTransferAgreementExists(consignmentId, appConfig)
  }

  "addTransferAgreementCompliance" should "return the expected data from inserted transfer agreement consignment metadata properties" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, userId)
      transferAgreementProperties.foreach(tap => utils.addConsignmentProperty(tap))
      val appConfig = config(container)

      val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_some")
      val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_somedata", validUserToken(), appConfig)

      response.data.get.addTransferAgreementCompliance should equal(expectedResponse.data.get.addTransferAgreementCompliance)

      checkTransferAgreementExists(consignmentId, appConfig)
  }

  "addTransferAgreementCompliance" should "throw an error if the consignment id field is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_consignmentid_missing")
      val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_missingconsignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addTransferAgreementCompliance" should "return an error if a user does not own the transfer agreement's consignment id" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val fixedUUIDSource = new FixedUUIDSource()
      val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, otherUserId)

      val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_error_not_owner")
      val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_alldata", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "addTransferAgreementCompliance" should "return an error if an invalid consignment id is provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      transferAgreementProperties.foreach(tap => utils.addConsignmentProperty(tap))
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId: UUID = fixedUUIDSource.uuid
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_error_invalid_consignmentid")
      val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_invalid_consignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def checkTransferAgreementExists(consignmentId: UUID, config: Config): Unit = {
    val sql = """SELECT * FROM "ConsignmentMetadata" cm JOIN "ConsignmentProperty" cp ON cp."Name" = cm."PropertyName" """ +
      """WHERE "ConsignmentId" = ? AND cp."Name" IN (?,?,?,?,?,?);"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    transferAgreementProperties.zipWithIndex.foreach {
      case (a, b) => ps.setString(b + 2, a)
    }
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
  }
}
