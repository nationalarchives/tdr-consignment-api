package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService.transferAgreementProperties
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, FixedUUIDSource, TestRequest}

class TransferAgreementRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase  {

  private val addTransferAgreementNotComplianceJsonFilePrefix: String = "json/addtransferagreementnotcompliance_"
  private val addTransferAgreementComplianceJsonFilePrefix: String = "json/addtransferagreementcompliance_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlTANotComplianceMutationData(data: Option[AddTransferAgreementNotCompliance], errors: List[GraphqlError] = Nil)
  case class GraphqlTAComplianceMutationData(data: Option[AddTransferAgreementCompliance], errors: List[GraphqlError] = Nil)
  case class TransferAgreementNotCompliance(
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
  case class AddTransferAgreementNotCompliance(addTransferAgreementNotCompliance: TransferAgreementNotCompliance) extends TestRequest
  case class AddTransferAgreementCompliance(addTransferAgreementCompliance: TransferAgreementCompliance) extends TestRequest

  val runTANotComplianceTestMutation: (String, OAuth2BearerToken) => GraphqlTANotComplianceMutationData =
    runTestRequest[GraphqlTANotComplianceMutationData](addTransferAgreementNotComplianceJsonFilePrefix)
  val expectedTANotComplianceMutationResponse: String => GraphqlTANotComplianceMutationData =
    getDataFromFile[GraphqlTANotComplianceMutationData](addTransferAgreementNotComplianceJsonFilePrefix)

  val runTAComplianceTestMutation: (String, OAuth2BearerToken) => GraphqlTAComplianceMutationData =
    runTestRequest[GraphqlTAComplianceMutationData](addTransferAgreementComplianceJsonFilePrefix)
  val expectedTAComplianceMutationResponse: String => GraphqlTAComplianceMutationData =
    getDataFromFile[GraphqlTAComplianceMutationData](addTransferAgreementComplianceJsonFilePrefix)


  "addTransferAgreementNotCompliance" should "return all requested fields from inserted transfer agreement consignment metadata properties" in {
    seedDatabaseWithDefaultEntries()
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTANotComplianceMutationData = expectedTANotComplianceMutationResponse("data_all")
    val response: GraphqlTANotComplianceMutationData = runTANotComplianceTestMutation("mutation_alldata", validUserToken())

    response.data.get.addTransferAgreementNotCompliance should equal(expectedResponse.data.get.addTransferAgreementNotCompliance)

    checkTransferAgreementExists(consignmentId)
  }

  "addTransferAgreementNotCompliance" should "return the expected data from inserted transfer agreement consignment metadata properties" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTANotComplianceMutationData = expectedTANotComplianceMutationResponse("data_some")
    val response: GraphqlTANotComplianceMutationData = runTANotComplianceTestMutation("mutation_somedata", validUserToken())

    response.data.get.addTransferAgreementNotCompliance should equal(expectedResponse.data.get.addTransferAgreementNotCompliance)

    checkTransferAgreementExists(consignmentId)
  }

  "addTransferAgreementNotCompliance" should "throw an error if the consignment id field is not provided" in {
    val expectedResponse: GraphqlTANotComplianceMutationData = expectedTANotComplianceMutationResponse("data_consignmentid_missing")
    val response: GraphqlTANotComplianceMutationData = runTANotComplianceTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal (expectedResponse.errors.head.message)
  }

  "addTransferAgreementNotCompliance" should "return an error if a user does not own the transfer agreement's consignment id" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, otherUserId)

    val expectedResponse: GraphqlTANotComplianceMutationData = expectedTANotComplianceMutationResponse("data_error_not_owner")
    val response: GraphqlTANotComplianceMutationData = runTANotComplianceTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "addTransferAgreementNotCompliance" should "return an error if an invalid consignment id is provided" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTANotComplianceMutationData = expectedTANotComplianceMutationResponse("data_error_invalid_consignmentid")
    val response: GraphqlTANotComplianceMutationData = runTANotComplianceTestMutation("mutation_invalid_consignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addTransferAgreementCompliance" should "return all requested fields from inserted transfer agreement consignment metadata properties" in {
    seedDatabaseWithDefaultEntries()
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_all")
    val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_alldata", validUserToken())

    response.data.get.addTransferAgreementCompliance should equal(expectedResponse.data.get.addTransferAgreementCompliance)

    checkTransferAgreementExists(consignmentId)
  }

  "addTransferAgreementCompliance" should "return the expected data from inserted transfer agreement consignment metadata properties" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_some")
    val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_somedata", validUserToken())

    response.data.get.addTransferAgreementCompliance should equal(expectedResponse.data.get.addTransferAgreementCompliance)

    checkTransferAgreementExists(consignmentId)
  }

  "addTransferAgreementCompliance" should "throw an error if the consignment id field is not provided" in {
    val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_consignmentid_missing")
    val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal (expectedResponse.errors.head.message)
  }

  "addTransferAgreementCompliance" should "return an error if a user does not own the transfer agreement's consignment id" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val otherUserId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, otherUserId)

    val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_error_not_owner")
    val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "addTransferAgreementCompliance" should "return an error if an invalid consignment id is provided" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId: UUID = fixedUUIDSource.uuid
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlTAComplianceMutationData = expectedTAComplianceMutationResponse("data_error_invalid_consignmentid")
    val response: GraphqlTAComplianceMutationData = runTAComplianceTestMutation("mutation_invalid_consignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def checkTransferAgreementExists(consignmentId: UUID): Unit = {
    val sql = "SELECT * FROM ConsignmentMetadata cm JOIN ConsignmentProperty cp ON cp.Name = cm.PropertyName " +
      "WHERE ConsignmentId = ? AND cp.Name IN (?,?,?,?,?,?);"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, consignmentId.toString)
    transferAgreementProperties.zipWithIndex.foreach {
      case (a, b) => ps.setString(b + 2, a)
    }
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
  }
}
