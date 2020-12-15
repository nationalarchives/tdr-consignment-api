package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestRequest}

class TransfersAgreementRouteSpec extends AnyFlatSpec with Matchers with TestRequest with BeforeAndAfterEach  {

  private val addTransferAgreementJsonFilePrefix: String = "json/addtransferagreement_"
  private val getTransferAgreementJsonFilePrefix: String = "json/gettransferagreement_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddTransferAgreement], errors: List[GraphqlError] = Nil)
  case class GraphqlQueryData(data: Option[TransferAgreement], errors: List[GraphqlError] = Nil)
  case class TransferAgreement(
                                consignmentid: Option[UUID] = None,
                                allPublicRecords: Option[Boolean] = None,
                                allCrownCopyright: Option[Boolean] = None,
                                allEnglish: Option[Boolean] = None,
                                appraisalSelectionSignedOff: Option[Boolean] = None,
                                sensitivityReviewSignedOff: Option[Boolean] = None,
                                transferAgreementId: Option[UUID] = None
                              )
  case class AddTransferAgreement(addTransferAgreement: TransferAgreement) extends TestRequest

  override def beforeEach(): Unit = {
    resetDatabase()
  }

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addTransferAgreementJsonFilePrefix)
  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData =
    runTestRequest[GraphqlQueryData](getTransferAgreementJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addTransferAgreementJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData =
    getDataFromFile[GraphqlQueryData](getTransferAgreementJsonFilePrefix)

  "The api" should "return all requested fields from inserted Transfer Agreement object" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, userId.toString)
    ps.executeUpdate()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.data.get.addTransferAgreement should equal(expectedResponse.data.get.addTransferAgreement)

    checkTransferAgreementExists(response.data.get.addTransferAgreement.transferAgreementId.get)
  }

  "The api" should "return the expected data from inserted Transfer Agreement object" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, userId.toString)
    ps.executeUpdate()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
    val response: GraphqlMutationData = runTestMutation("mutation_somedata", validUserToken())
    response.data.get.addTransferAgreement should equal(expectedResponse.data.get.addTransferAgreement)

    checkTransferAgreementExists(response.data.get.addTransferAgreement.transferAgreementId.get)
  }

  "The api" should "throw an error if the consignment id field is not provided" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_consignmentid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal (expectedResponse.errors.head.message)
  }

  "The api" should "return an error if a user does not own the transfer agreement's consignment id" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, "5ab14990-ed63-4615-8336-56fbb9960300")
    ps.executeUpdate()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_not_owner")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "return an error if an invalid consignment id is provided" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, userId.toString)
    ps.executeUpdate()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_invalid_consignmentid")
    val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an existing transfer agreement for a user owned consignment" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentSql = s"insert into Consignment (SeriesId, UserId) VALUES (1,'$userId')"
    val sql = "INSERT INTO TransferAgreement (ConsignmentId, AllPublicRecords, AllCrownCopyright, " +
      "AllEnglish, AllDigital, AppraisalSelectionSignedOff, SensitivityReviewSignedOff, TransferAgreementId) " +
      "VALUES (?, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, ?);"
    val psConsignment: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(consignmentSql)
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    val uuid = fixedUUIDSource.uuid.toString
    ps.setString(1, uuid)
    ps.setString(2, uuid)
    ps.executeUpdate()
    psConsignment.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return no transfer agreement if it doesn't exist" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, userId.toString)
    ps.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_none")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "The api" should "return an error if the consignment id isn't provided" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, userId.toString)
    ps.executeUpdate()
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_consignmentidmissing")
    val response: GraphqlQueryData = runTestQuery("query_missingconsignmentid", validUserToken())
    response.errors.length should equal(1)
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "The api" should "return an error if the user doesn't own the consignment" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val otherUserId = UUID.randomUUID().toString
    val sql = "insert into Consignment (SeriesId, UserId) VALUES (?,?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fixedUUIDSource.uuid.toString)
    ps.setString(2, otherUserId)
    ps.executeUpdate()

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_notowner")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken())
    response.errors.length should equal(1)
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  private def checkTransferAgreementExists(transferAgreementId: UUID): Unit = {
    val sql = "select * from TransferAgreement where TransferAgreementId = ?"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, transferAgreementId.toString)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("TransferAgreementId") should equal(transferAgreementId.toString)
  }

  private def resetDatabase(): Unit = {
    DbConnection.db.source.createConnection().prepareStatement("delete from TransferAgreement").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("delete from Consignment").executeUpdate()
  }
}
