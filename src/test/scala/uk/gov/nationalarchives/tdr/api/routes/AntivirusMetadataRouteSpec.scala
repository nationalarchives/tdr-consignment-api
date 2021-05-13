package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestRequest}

class AntivirusMetadataRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase  {

  private val addAVMetadataJsonFilePrefix: String = "json/addavmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddAntivirusMetadata], errors: List[GraphqlError] = Nil)
  case class AntivirusMetadata(
                                fileId: UUID,
                                software: Option[String] = None,
                                softwareVersion: Option[String] = None,
                                databaseVersion: Option[String] = None,
                                result: Option[String] = None,
                                datetime: Long
                              )
  case class AddAntivirusMetadata(addAntivirusMetadata: AntivirusMetadata) extends TestRequest

  override def beforeEach(): Unit = {
    super.beforeEach()
    seedDatabaseWithDefaultEntries()
  }

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addAVMetadataJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addAVMetadataJsonFilePrefix)

  "addAntivirusMetadata" should "return all requested fields from inserted antivirus metadata object" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))
    response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

    checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId)
  }

  "addAntivirusMetadata" should "return the expected data from inserted antivirus metadata object" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
    val response: GraphqlMutationData = runTestMutation("mutation_somedata", validBackendChecksToken("antivirus"))
    response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

    checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId)
  }

  "addAntivirusMetadata" should "not allow updating of antivirus metadata with incorrect authorisation" in {
    val response: GraphqlMutationData = runTestMutation("mutation_somedata", invalidBackendChecksToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoAntivirusMetadataAdded()
  }

  "addAntivirusMetadata" should "throw an error if the field file id is not provided" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingfileid", validBackendChecksToken("antivirus"))

    response.errors.head.message should equal (expectedResponse.errors.head.message)
    checkNoAntivirusMetadataAdded()
  }

  "addAntivirusMetadata" should "set the file status to virus found when there is a virus found" in {
    runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

    val result = getFileStatusResult(defaultFileId, Antivirus)
    result.size should be(1)
    result.head should equal(VirusDetected)
  }

  "addAntivirusMetadata" should "set the file status to success when there is no virus found" in {
    runTestMutation("mutation_noresult", validBackendChecksToken("antivirus"))

    val result = getFileStatusResult(defaultFileId, Antivirus)
    result.size should be(1)
    result.head should equal(Success)
  }

  private def checkAntivirusMetadataExists(fileId: UUID): Unit = {
    val sql = "select * from AVMetadata where FileId = ?;"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId.toString)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNoAntivirusMetadataAdded(): Unit = {
    val sql = "select * from AVMetadata;"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs.last()
    rs.getRow should equal(0)
  }
}
