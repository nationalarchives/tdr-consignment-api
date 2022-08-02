package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{BulkFileMetadata, FileMetadataWithFileId, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ChecksumMatch, Success}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileMetadataJsonFilePrefix: String = "json/addfilemetadata_"
  private val addElseUpdateBulkFileMetadataJsonFilePrefix: String = "json/addelseupdatebulkfilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlAddFileMetadataMutationData(data: Option[AddFileMetadata], errors: List[GraphqlError] = Nil)
  case class GraphqlAddElseUpdateBulkFileMetadataMutationData(data: Option[AddElseUpdateBulkFileMetadata], errors: List[GraphqlError] = Nil)

  case class AddFileMetadata(addFileMetadata: FileMetadataWithFileId)
  case class AddElseUpdateBulkFileMetadata(addElseUpdateBulkFileMetadata: BulkFileMetadata)

  val runAddFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileMetadataMutationData =
    runTestRequest[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val expectedAddFileMetadataMutationResponse: String => GraphqlAddFileMetadataMutationData =
    getDataFromFile[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val runAddElseUpdateBulkFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddElseUpdateBulkFileMetadataMutationData =
    runTestRequest[GraphqlAddElseUpdateBulkFileMetadataMutationData](addElseUpdateBulkFileMetadataJsonFilePrefix)

  val expectedAddElseUpdateBulkFileMetadataMutationResponse: String => GraphqlAddElseUpdateBulkFileMetadataMutationData =
    getDataFromFile[GraphqlAddElseUpdateBulkFileMetadataMutationData](addElseUpdateBulkFileMetadataJsonFilePrefix)

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
      utils.addFileProperty(SHA256ServerSideChecksum)
      runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("checksum"))

      val result = utils.getFileStatusResult(defaultFileId, ChecksumMatch)
      result.size should be(1)
      result.head should equal(Success)
  }

  "addFileMetadata" should "add the checksum validation result if this is a checksum update and the checksum doesn't match" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runAddFileMetadataTestMutation("mutation_mismatch_checksum", validBackendChecksToken("checksum"))
      utils.getFileStatusResult(defaultFileId, ChecksumMatch)
  }

  "addFileMetadata" should "not add the checksum validation result if this is not a checksum update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.seedDatabaseWithDefaultEntries()
      runAddFileMetadataTestMutation("mutation_notchecksum", validBackendChecksToken("checksum"))
      checkNoValidationResultExists(defaultFileId, utils)
  }

  "addElseUpdateBulkFileMetadata" should "return all fileIds and the properties that were passed in " +
    "(as no metadata rows, belonging to the ids, had the same values the user is trying to add/update)" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")

      // folderOneId WILL be passed into addElseUpdateBulkFileMetadata as it is inside but it will NOT be returned since no metadata was applied to it
      utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
      // fileOneId will NOT be passed into addElseUpdateBulkFileMetadata as it is inside "folderName" but it WILL be returned since metadata was applied to it
      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "property1", "oldvalue1")

      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData = expectedAddElseUpdateBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileIds = expectedResponse.data.get.addElseUpdateBulkFileMetadata.fileIds
      val expectedResponseFileMetadata = expectedResponse.data.get.addElseUpdateBulkFileMetadata.metadataProperties
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData = runAddElseUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      val responseFileIds: Seq[UUID] = response.data.get.addElseUpdateBulkFileMetadata.fileIds
      val responseFileMetadataProperties = response.data.get.addElseUpdateBulkFileMetadata.metadataProperties
      val parentIdOfFileOneId: UUID = UUID.fromString(getParentId(fileOneId, utils))

      responseFileIds.contains(folderOneId) should equal(false)
      responseFileIds.contains(fileOneId) should equal(true)
      parentIdOfFileOneId should equal(folderOneId)

      val correctPropertiesWerePassedIn: Boolean = responseFileMetadataProperties.forall(
        fileMetadata => expectedResponseFileMetadata.contains(fileMetadata)
      )

      correctPropertiesWerePassedIn should equal(true)
      responseFileIds.sorted should equal(expectedResponseFileIds.sorted)
      responseFileIds.foreach(fileId =>
        responseFileMetadataProperties.foreach(fileMetadata =>
          checkFileMetadataExists(fileId, utils, fileMetadata.filePropertyName)
        )
      )
  }

  "addElseUpdateBulkFileMetadata" should "return only fileIds and the properties that were added " +
    "(excluding ones where metadata rows, belonging to the ids, had the same values the user is trying to add/update)" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")

      // folderOneId WILL be passed into addElseUpdateBulkFileMetadata as it is inside but it will NOT be returned since no metadata was applied to it
      utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
      // fileOneId will NOT be passed into addElseUpdateBulkFileMetadata as it is inside "folderName" but it WILL be returned since metadata was applied to it
      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "property1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "property2", "value2")

      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        expectedAddElseUpdateBulkFileMetadataMutationResponse("data_values_already_exist_on_file")
      val expectedResponseFileIds = expectedResponse.data.get.addElseUpdateBulkFileMetadata.fileIds
      val expectedResponseFileMetadata = expectedResponse.data.get.addElseUpdateBulkFileMetadata.metadataProperties
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        runAddElseUpdateBulkFileMetadataTestMutation("mutation_valuesalreadyexistonfile", validUserToken())
      val responseFileIds: Seq[UUID] = response.data.get.addElseUpdateBulkFileMetadata.fileIds
      val responseFileMetadataProperties = response.data.get.addElseUpdateBulkFileMetadata.metadataProperties
      val parentIdOfFileOneId: UUID = UUID.fromString(getParentId(fileOneId, utils))

      responseFileIds.contains(folderOneId) should equal(false)
      responseFileIds.contains(fileOneId) should equal(true)
      parentIdOfFileOneId should equal(folderOneId)

      val correctPropertiesWerePassedIn: Boolean = responseFileMetadataProperties.forall(
        fileMetadata => expectedResponseFileMetadata.contains(fileMetadata)
      )

      correctPropertiesWerePassedIn should equal(true)
      responseFileIds.sorted should equal(expectedResponseFileIds.sorted)
      responseFileIds.foreach(fileId =>
        responseFileMetadataProperties.foreach(fileMetadata =>
          checkFileMetadataExists(fileId, utils, fileMetadata.filePropertyName)
        )
      )
  }

  "addElseUpdateBulkFileMetadata" should "not allow bulk updating of file metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
      val token = validUserToken(wrongUserId)
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData = runAddElseUpdateBulkFileMetadataTestMutation("mutation_alldata", token)

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addElseUpdateBulkFileMetadata" should "throw an error if the field fileIds is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData = expectedAddElseUpdateBulkFileMetadataMutationResponse("data_fileids_missing")
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData = runAddElseUpdateBulkFileMetadataTestMutation("mutation_missingfileids", validUserToken())

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addElseUpdateBulkFileMetadata" should "throw an error if the field metadataProperties is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        expectedAddElseUpdateBulkFileMetadataMutationResponse("data_metadataproperties_missing")
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        runAddElseUpdateBulkFileMetadataTestMutation("mutation_missingmetadataproperties", validUserToken())

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addElseUpdateBulkFileMetadata" should "throw an error if some file ids do not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")
      // folderOneId WILL be passed into addElseUpdateBulkFileMetadata as it is inside but it will NOT be returned since no metadata was applied to it
      utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
      // fileOneId will NOT be passed into addElseUpdateBulkFileMetadata as it is inside "folderName" but it WILL be returned since metadata was applied to it
      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)

      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData = expectedAddElseUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        runAddElseUpdateBulkFileMetadataTestMutation("mutation_fileidnotexists", validUserToken())

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addElseUpdateBulkFileMetadata" should "throw an error if a file id exists but belongs to another user" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.fromString("373ce1c5-6e06-423d-8b86-ca5eaebef457")
      val fileFiveId = UUID.fromString("5302acac-1396-44fe-9094-dc262414a03a")

      utils.addFileProperty("property1")
      utils.addFileProperty("property2")
      // folderOneId WILL be passed into addElseUpdateBulkFileMetadata as it is inside but it will NOT be returned since no metadata was applied to it
      utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
      // fileOneId will NOT be passed into addElseUpdateBulkFileMetadata as it is inside "folderName" but it WILL be returned since metadata was applied to it
      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      val consignmentId2 = UUID.fromString("3a4d1650-dc96-4b0d-a2e7-3551a682b46f")
      val consignmentId3 = UUID.fromString("75ec3c85-ba66-4145-842f-0aa91b1a9972")
      val userId2 = UUID.fromString("a2c292e8-e764-4dd5-99eb-23084c226013")
      val userId3 = UUID.fromString("c83b64c8-b7f5-47e2-94a4-4b91bf76faea")
      utils.createConsignment(consignmentId2, userId = userId2)
      utils.createFile(fileFourId, consignmentId2, userId = userId2)
      utils.createConsignment(consignmentId3, userId = userId3)
      utils.createFile(fileFiveId, consignmentId3, userId = userId3)

      val expectedResponse: GraphqlAddElseUpdateBulkFileMetadataMutationData =
        expectedAddElseUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
      val response: GraphqlAddElseUpdateBulkFileMetadataMutationData = runAddElseUpdateBulkFileMetadataTestMutation("mutation_notfileowner", validUserToken())

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  private def getParentId(fileId: UUID, utils: TestUtils): String = {
    val sql = """SELECT * FROM "File" WHERE "FileId" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ParentId")
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

  private def checkNoFileMetadataAdded(utils: TestUtils, propertyName: String=SHA256ServerSideChecksum): Unit = {
    val sql = """select * from "FileMetadata" WHERE "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setString(1, propertyName)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
  }

  private def checkNoValidationResultExists(fileId: UUID, utils: TestUtils): Unit = {
    val sql = s"""SELECT COUNT("Value") FROM "FileStatus" where "FileId" = ? AND "StatusType" = ?"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, ChecksumMatch)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getInt(1) should be(0)
  }
}
