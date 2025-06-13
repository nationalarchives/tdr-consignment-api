package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{FileMetadataWithFileId, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest with TableDrivenPropertyChecks {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileMetadataJsonFilePrefix: String = "json/addfilemetadata_"
  private val addOrUpdateBulkFileMetadataJsonFilePrefix: String = "json/addorupdatebulkfilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlAddFileMetadataMutationData(data: Option[AddFileMetadata], errors: List[GraphqlError] = Nil)

  case class GraphqlAddOrUpdateBulkFileMetadataMutationData(data: Option[AddOrUpdateBulkFileMetadata], errors: List[GraphqlError] = Nil)

  case class AddFileMetadata(addMultipleFileMetadata: List[FileMetadataWithFileId])

  case class AddOrUpdateBulkFileMetadata(addOrUpdateBulkFileMetadata: List[FileMetadataWithFileId])

  val runAddFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileMetadataMutationData =
    runTestRequest[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val expectedAddFileMetadataMutationResponse: String => GraphqlAddFileMetadataMutationData =
    getDataFromFile[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val runAddOrUpdateBulkFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddOrUpdateBulkFileMetadataMutationData =
    runTestRequest[GraphqlAddOrUpdateBulkFileMetadataMutationData](addOrUpdateBulkFileMetadataJsonFilePrefix)

  val expectedAddOrUpdateBulkFileMetadataMutationResponse: String => GraphqlAddOrUpdateBulkFileMetadataMutationData =
    getDataFromFile[GraphqlAddOrUpdateBulkFileMetadataMutationData](addOrUpdateBulkFileMetadataJsonFilePrefix)

  "addFileMetadata" should "return all requested fields from inserted checksum file metadata object" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    utils.addFileProperty(SHA256ServerSideChecksum)
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_all")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("checksum"))
    response.data.get.addMultipleFileMetadata.head should equal(expectedResponse.data.get.addMultipleFileMetadata.head)

    checkFileMetadataExists(response.data.get.addMultipleFileMetadata.head.fileId, utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect authorisation" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", invalidBackendChecksToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "not allow updating of file metadata with incorrect client role" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file property name is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileproperty_missing")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingfileproperty", validBackendChecksToken("checksum"))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the field file id is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileid_missing")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingfileid", validBackendChecksToken("checksum"))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the value is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_value_missing")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_missingvalue", validBackendChecksToken("checksum"))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file id does not exist" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    utils.addFileProperty(SHA256ServerSideChecksum)
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_fileid_not_exists")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_fileidnotexists", validBackendChecksToken("checksum"))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils)
  }

  "addFileMetadata" should "throw an error if the file property does not exist" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val expectedResponse: GraphqlAddFileMetadataMutationData = expectedAddFileMetadataMutationResponse("data_incorrect_property")
    val response: GraphqlAddFileMetadataMutationData = runAddFileMetadataTestMutation("mutation_incorrectproperty", validBackendChecksToken("checksum"))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils)
  }

  "addOrUpdateBulkFileMetadata" should "add or update all file metadata based on input for all file ids and update the statuses" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("newProperty1", propertyType = "Supplied")
      utils.addFileProperty("existingPropertyUpdated1", propertyType = "Supplied")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyUpdated1", "existingValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyUpdated1", "newValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "existingValue1")

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileMetadata = expectedResponse.data.get
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      val responseFileMetadataProperties = response.data.get

      responseFileMetadataProperties should equal(expectedResponseFileMetadata)

      List(fileOneId, fileTwoId, fileThreeId).foreach { id =>
        checkFileMetadataValue(id, utils, "newProperty1", "value1")
        checkFileMetadataValue(id, utils, "existingPropertyUpdated1", "newValue1")
      }
  }

  "addOrUpdateBulkFileMetadata" should "add or update all file metadata without validation and update the statuses when skipValidation is false" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("newProperty1", propertyType = "Supplied")
      utils.addFileProperty("existingPropertyUpdated1", propertyType = "Supplied")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyUpdated1", "existingValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyUpdated1", "newValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "existingValue1")

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileMetadata = expectedResponse.data.get
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_with_validation", validUserToken())
      val responseFileMetadataProperties = response.data.get

      responseFileMetadataProperties should equal(expectedResponseFileMetadata)

      List(fileOneId, fileTwoId, fileThreeId).foreach { id =>
        checkFileMetadataValue(id, utils, "newProperty1", "value1")
        checkFileMetadataValue(id, utils, "existingPropertyUpdated1", "newValue1")
      }
  }

  "addOrUpdateBulkFileMetadata" should "throw an error when the user tries to add or update all file metadata when skipValidation is true" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("newProperty1", propertyType = "Supplied")
      utils.addFileProperty("existingPropertyUpdated1", propertyType = "Supplied")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyUpdated1", "existingValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyUpdated1", "newValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "existingValue1")

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_skip_validation", validUserToken())

      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
  }

  "addOrUpdateBulkFileMetadata" should "return an error if trying to add metadata that is protected" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

    val fileId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    utils.addFileProperty("newProperty1")
    utils.createFile(fileId, consignmentId)

    val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      expectedAddOrUpdateBulkFileMetadataMutationResponse("data_protected")
    val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      runAddOrUpdateBulkFileMetadataTestMutation("mutation_protected", validUserToken())

    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addOrUpdateBulkFileMetadata" should "not allow bulk updating of non-directories file metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
      val token = validUserToken(wrongUserId)
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)

      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData = runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", token)

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw an error if the field fileIds is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData = expectedAddOrUpdateBulkFileMetadataMutationResponse("data_fileids_missing")
    val response: GraphqlAddOrUpdateBulkFileMetadataMutationData = runAddOrUpdateBulkFileMetadataTestMutation("mutation_missingfileids", validUserToken())

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw an error if the field metadataProperties is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      expectedAddOrUpdateBulkFileMetadataMutationResponse("data_metadataproperties_missing")
    val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      runAddOrUpdateBulkFileMetadataTestMutation("mutation_missingmetadataproperties", validUserToken())

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw an 'invalid input data' error if some file ids do not exist" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    utils.addFileProperty("property1")

    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId)

    val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData = expectedAddOrUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
    val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      runAddOrUpdateBulkFileMetadataTestMutation("mutation_fileidnotexists", validUserToken())

    response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
  }

  "addOrUpdateBulkFileMetadata" should "throw an 'invalid input data' error if the consignment input id does not match the files' consignment id" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData = expectedAddOrUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_different_consignmentid", validUserToken())

      response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw an 'invalid input data' error if input file ids belong to different consignments" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val differentConsignmentId = UUID.fromString("7ece007d-6f47-4be2-895f-511a607e4074")
      utils.createConsignment(differentConsignmentId)

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("property1")
      utils.addFileProperty("property2")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, differentConsignmentId)
      utils.createFile(fileThreeId, consignmentId)

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())

      response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw a 'not authorised' error if a file id exists but belongs to another user" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
    val fileFourId = UUID.fromString("373ce1c5-6e06-423d-8b86-ca5eaebef457")
    val fileFiveId = UUID.fromString("5302acac-1396-44fe-9094-dc262414a03a")

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

    val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
      expectedAddOrUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
    val response: GraphqlAddOrUpdateBulkFileMetadataMutationData = runAddOrUpdateBulkFileMetadataTestMutation("mutation_notfileowner", validUserToken())

    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "throw a 'not authorised' error if a file id exists but belongs to another user and skipValidation is true" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.fromString("373ce1c5-6e06-423d-8b86-ca5eaebef457")
      val fileFiveId = UUID.fromString("5302acac-1396-44fe-9094-dc262414a03a")

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

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData = runAddOrUpdateBulkFileMetadataTestMutation("mutation_notfileowner_skip_validation", validUserToken())

      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "addOrUpdateBulkFileMetadata" should "add or update all file metadata when consignment belongs to another user when called by draft metadata client" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("newProperty1", propertyType = "Supplied")
      utils.addFileProperty("existingPropertyUpdated1", propertyType = "Supplied")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyUpdated1", "existingValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyUpdated1", "newValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "existingValue1")

      val updateMetadataAccessToken = validDraftMetadataToken("update_metadata")

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileMetadata = expectedResponse.data.get
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", updateMetadataAccessToken)
      val responseFileMetadataProperties = response.data.get

      responseFileMetadataProperties should equal(expectedResponseFileMetadata)
  }

  "addOrUpdateBulkFileMetadata" should "throw a 'not authorised' exception is thrown when the user doesn't own the file and the draft metadata client is not used" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("373ce1c5-6e06-423d-8b86-ca5eaebef457")
      val fileFourId = UUID.fromString("5302acac-1396-44fe-9094-dc262414a03a")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)

      val consignmentId2 = UUID.fromString("3a4d1650-dc96-4b0d-a2e7-3551a682b46f")
      val consignmentId3 = UUID.fromString("75ec3c85-ba66-4145-842f-0aa91b1a9972")
      val userId2 = UUID.fromString("a2c292e8-e764-4dd5-99eb-23084c226013")
      val userId3 = UUID.fromString("c83b64c8-b7f5-47e2-94a4-4b91bf76faea")
      utils.createConsignment(consignmentId2, userId = userId2)
      utils.createFile(fileThreeId, consignmentId2, userId = userId2)
      utils.createConsignment(consignmentId3, userId = userId3)
      utils.createFile(fileFourId, consignmentId3, userId = userId3)

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData = runAddOrUpdateBulkFileMetadataTestMutation("mutation_notfileowner", validUserToken())

      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  private def checkFileMetadataExists(fileId: UUID, utils: TestUtils, propertyName: String = SHA256ServerSideChecksum): Unit = {
    val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, propertyName)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkFileMetadataValue(fileId: UUID, utils: TestUtils, propertyName: String, propertyValue: String): Unit = {
    val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, propertyName)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("Value") should equal(propertyValue)
  }

  private def checkNoFileMetadataAdded(utils: TestUtils, propertyName: String = SHA256ServerSideChecksum): Unit = {
    val sql = """select * from "FileMetadata" WHERE "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setString(1, propertyName)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
  }

}
