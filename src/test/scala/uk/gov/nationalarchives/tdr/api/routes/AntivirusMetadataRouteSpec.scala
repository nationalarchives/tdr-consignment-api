package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class AntivirusMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

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

  val runTestMutation: (String, OAuth2BearerToken, Config) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addAVMetadataJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addAVMetadataJsonFilePrefix)

  "addAntivirusMetadata" should "return all requested fields from inserted antivirus metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"), config(container))
      response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

      checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId, config(container))
  }

  "addAntivirusMetadata" should "return the expected data from inserted antivirus metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", validBackendChecksToken("antivirus"), config(container))
      response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

      checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId, config(container))
  }

  "addAntivirusMetadata" should "not allow updating of antivirus metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", invalidBackendChecksToken(), config(container))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoAntivirusMetadataAdded(config(container))
  }

  "addAntivirusMetadata" should "throw an error if the field file id is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingfileid", validBackendChecksToken("antivirus"), config(container))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoAntivirusMetadataAdded(config(container))
  }

  "addAntivirusMetadata" should "set the file status to virus found when there is a virus found" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.seedDatabaseWithDefaultEntries()
      runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"), config(container))

      val result = utils.getFileStatusResult(defaultFileId, Antivirus)
      result.size should be(1)
      result.head should equal(VirusDetected)
  }

  "addAntivirusMetadata" should "set the file status to success when there is no virus found" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.seedDatabaseWithDefaultEntries()
      runTestMutation("mutation_noresult", validBackendChecksToken("antivirus"), config(container))

      val result = utils.getFileStatusResult(defaultFileId, Antivirus)
      result.size should be(1)
      result.head should equal(Success)
  }

  private def checkAntivirusMetadataExists(fileId: UUID, config: Config): Unit = {
    val sql = """select * from "AVMetadata" where "FileId" = ?;"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNoAntivirusMetadataAdded(config: Config): Unit = {
    val sql = """select * from "AVMetadata";"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
  }
}
