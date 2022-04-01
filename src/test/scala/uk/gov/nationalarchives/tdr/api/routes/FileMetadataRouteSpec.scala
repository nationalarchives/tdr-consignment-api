package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{BulkFileMetadata, FileMetadataWithFileId, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Checksum, Success}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileMetadataJsonFilePrefix: String = "json/addfilemetadata_"
  private val addBulkFileMetadataJsonFilePrefix: String = "json/addbulkfilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlAddFileMetadataMutationData(data: Option[AddFileMetadata], errors: List[GraphqlError] = Nil)
  case class GraphqlAddBulkFileMetadataMutationData(data: Option[AddBulkFileMetadata], errors: List[GraphqlError] = Nil)

  case class AddFileMetadata(addFileMetadata: FileMetadataWithFileId)
  case class AddBulkFileMetadata(addBulkFileMetadata: BulkFileMetadata)

  val runAddFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileMetadataMutationData =
    runTestRequest[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val expectedAddFileMetadataMutationResponse: String => GraphqlAddFileMetadataMutationData =
    getDataFromFile[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val runAddBulkFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileMetadataMutationData =
    runTestRequest[GraphqlAddFileMetadataMutationData](addBulkFileMetadataJsonFilePrefix)

  val expectedAddBulkFileMetadataMutationResponse: String => GraphqlAddBulkFileMetadataMutationData =
    getDataFromFile[GraphqlAddBulkFileMetadataMutationData](addBulkFileMetadataJsonFilePrefix)

  "addFileMetadata" should "return all requested fields from inserted checksum file metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      utils.addFileProperty(SHA256ServerSideChecksum)
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_all")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("checksum"))
      response.data.get.addFileMetadata should equal(expectedResponse.data.get.addFileMetadata)

      checkFileMetadataExists(response.data.get.addFileMetadata.fileId, utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", invalidBackendChecksToken())

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect client role" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file property name is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileproperty_missing")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingfileproperty", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file id is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileid_missing")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingfileid", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the value is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_value_missing")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingvalue", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file id does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_fileidnotexists", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file property does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_incorrect_property")
      val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_incorrectproperty", validBackendChecksToken("checksum"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "add the checksum validation result if this is a checksum update and the checksum matches" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("checksum"))

      val result = utils.getFileStatusResult(defaultFileId, Checksum)
      result.size should be(1)
      result.head should equal(Success)
  }

  "addFileMetadata" should "add the checksum validation result if this is a checksum update and the checksum doesn't match" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runAddFileMetadataTestMutation("mutation_mismatch_checksum", validBackendChecksToken("checksum"))
      utils.getFileStatusResult(defaultFileId, Checksum)
  }

  "addFileMetadata" should "not add the checksum validation result if this is not a checksum update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runAddFileMetadataTestMutation("mutation_notchecksum", validBackendChecksToken("checksum"))
      checkNoValidationResultExists(defaultFileId, utils)
  }

  "addBulkFileMetadata" should "return fileIds for all files where metadata was added and the properties that were added" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")

      // folderOneId WILL be passed into addBulkFileMetadata as it is inside but it will NOT be returned since no metadata was applied to it
      utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
      // fileOneId will NOT be passed into addBulkFileMetadata as it is inside "folderName" but it WILL be returned since metadata was applied to it
      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      val expectedResponse: GraphqlAddBulkFileMetadataMutationData = expectedAddBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileIds = expectedResponse.data.get.addBulkFileMetadata.fileIds
      val expectedResponseFileMetadata = expectedResponse.data.get.addBulkFileMetadata.metadataProperties
      val response: GraphqlAddBulkFileMetadataMutationData = runAddBulkFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("checksum"))
      val responseFileIds: Seq[UUID] = response.data.get.addBulkFileMetadata.fileIds
      val responseFileMetadataProperties = response.data.get.addBulkFileMetadata.metadataProperties

      val correctPropertiesWerePassedIn: Boolean = responseFileMetadataProperties.forall(
        fileMetadata => expectedResponseFileMetadata.contains(fileMetadata)
      )
      correctPropertiesWerePassedIn should be true

      response.data.get.addBulkFileMetadata should equal(expectedResponse.data.get.addBulkFileMetadata)
      responseFileIds.foreach(fileId =>
        responseFileMetadataProperties.
      )
      checkFileMetadataExists(response.data.get.addBulkFileMetadata., utils)
  }

  private def checkFileMetadataExists(fileId: UUID, utils: TestUtils, propertyName: String=SHA256ServerSideChecksum): Unit = {
    val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, propertyName)
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
