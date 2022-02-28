package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{FileMetadata, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Checksum, Success}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileMetadataJsonFilePrefix: String = "json/addfilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlMutationData(data: Option[AddFileMetadata], errors: List[GraphqlError] = Nil)

  case class AddFileMetadata(addFileMetadata: FileMetadata)

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addFileMetadataJsonFilePrefix)

  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addFileMetadataJsonFilePrefix)

  "addFileMetadata" should "return all requested fields from inserted checksum file metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      utils.addFileProperty(SHA256ServerSideChecksum)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("checksum"))
      response.data.get.addFileMetadata should equal(expectedResponse.data.get.addFileMetadata)

      checkFileMetadataExists(response.data.get.addFileMetadata.fileId, utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", invalidBackendChecksToken())

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect client role" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file property name is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileproperty_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingfileproperty", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file id is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingfileid", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the value is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_value_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingvalue", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file id does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_not_exists")
      val response: GraphqlMutationData = runTestMutation("mutation_fileidnotexists", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file property does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_incorrect_property")
      val response: GraphqlMutationData = runTestMutation("mutation_incorrectproperty", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "add the checksum validation result if this is a checksum update and the checksum matches" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runTestMutation("mutation_alldata", validBackendChecksToken("checksum"))

      val result = utils.getFileStatusResult(defaultFileId, Checksum)
      result.size should be(1)
      result.head should equal(Success)
  }

  "addFileMetadata" should "add the checksum validation result if this is a checksum update and the checksum doesn't match" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runTestMutation("mutation_mismatch_checksum", validBackendChecksToken("checksum"))
      utils.getFileStatusResult(defaultFileId, Checksum)
  }

  "addFileMetadata" should "not add the checksum validation result if this is not a checksum update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runTestMutation("mutation_notchecksum", validBackendChecksToken("checksum"))
      checkNoValidationResultExists(defaultFileId, utils)
  }

  private def checkFileMetadataExists(fileId: UUID, utils: TestUtils): Unit = {
    val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, SHA256ServerSideChecksum)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNoFileMetadataAdded(utils: TestUtils): Unit = {
    val sql = """select * from "FileMetadata" WHERE "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setString(1, SHA256ServerSideChecksum)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
  }

  private def checkNoValidationResultExists(fileId: UUID, utils: TestUtils): Unit = {
    val sql = s"""SELECT COUNT("Value") FROM "FileStatus" where "FileId" = ? AND "StatusType" = ?"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, Checksum)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getInt(1) should be(0)
  }
}
