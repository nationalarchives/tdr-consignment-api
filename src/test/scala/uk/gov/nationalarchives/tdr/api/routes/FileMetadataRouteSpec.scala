package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{BulkFileMetadata, DeleteFileMetadata, FileMetadataWithFileId, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

//scalastyle:off method.length
class FileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest with TableDrivenPropertyChecks {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileMetadataJsonFilePrefix: String = "json/addfilemetadata_"
  private val updateBulkFileMetadataJsonFilePrefix: String = "json/updatebulkfilemetadata_"
  private val addOrUpdateBulkFileMetadataJsonFilePrefix: String = "json/addorupdatebulkfilemetadata_"
  private val deleteFileMetadataJsonFilePrefix: String = "json/deletefilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlAddFileMetadataMutationData(data: Option[AddFileMetadata], errors: List[GraphqlError] = Nil)

  case class GraphqlUpdateBulkFileMetadataMutationData(data: Option[UpdateBulkFileMetadata], errors: List[GraphqlError] = Nil)

  case class GraphqlAddOrUpdateBulkFileMetadataMutationData(data: Option[AddOrUpdateBulkFileMetadata], errors: List[GraphqlError] = Nil)

  case class GraphqlDeleteFileMetadataMutationData(data: Option[DeletedFileMetadata], errors: List[GraphqlError] = Nil)

  case class AddFileMetadata(addMultipleFileMetadata: List[FileMetadataWithFileId])

  case class AddOrUpdateBulkFileMetadata(addOrUpdateBulkFileMetadata: List[FileMetadataWithFileId])

  case class UpdateBulkFileMetadata(updateBulkFileMetadata: BulkFileMetadata)

  case class DeletedFileMetadata(deleteFileMetadata: DeleteFileMetadata)

  val runAddFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileMetadataMutationData =
    runTestRequest[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val expectedAddFileMetadataMutationResponse: String => GraphqlAddFileMetadataMutationData =
    getDataFromFile[GraphqlAddFileMetadataMutationData](addFileMetadataJsonFilePrefix)

  val runUpdateBulkFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlUpdateBulkFileMetadataMutationData =
    runTestRequest[GraphqlUpdateBulkFileMetadataMutationData](updateBulkFileMetadataJsonFilePrefix)

  val expectedUpdateBulkFileMetadataMutationResponse: String => GraphqlUpdateBulkFileMetadataMutationData =
    getDataFromFile[GraphqlUpdateBulkFileMetadataMutationData](updateBulkFileMetadataJsonFilePrefix)

  val runAddOrUpdateBulkFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlAddOrUpdateBulkFileMetadataMutationData =
    runTestRequest[GraphqlAddOrUpdateBulkFileMetadataMutationData](addOrUpdateBulkFileMetadataJsonFilePrefix)

  val expectedAddOrUpdateBulkFileMetadataMutationResponse: String => GraphqlAddOrUpdateBulkFileMetadataMutationData =
    getDataFromFile[GraphqlAddOrUpdateBulkFileMetadataMutationData](addOrUpdateBulkFileMetadataJsonFilePrefix)

  val runDeleteFileMetadataTestMutation: (String, OAuth2BearerToken) => GraphqlDeleteFileMetadataMutationData =
    runTestRequest[GraphqlDeleteFileMetadataMutationData](deleteFileMetadataJsonFilePrefix)

  val expectedDeleteFileMetadataMutationResponse: String => GraphqlDeleteFileMetadataMutationData =
    getDataFromFile[GraphqlDeleteFileMetadataMutationData](deleteFileMetadataJsonFilePrefix)

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
      utils.addFileProperty("newProperty1")
      utils.addFileProperty("existingPropertyUpdated1")

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

  "addOrUpdateBulkFileMetadata" should "set the expected consignment and file statuses for all input file ids" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()
    val propertyGroup = "MandatoryClosure"

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
    utils.addFileProperty("ClosureType", propertyGroup)
    utils.addFileProperty("newProperty1", propertyGroup)
    utils.addFileProperty("existingPropertyUpdated1", propertyGroup)

    utils.createDisplayProperty("newProperty1", "Active", "true", "boolean")
    utils.createDisplayProperty("existingPropertyUpdated1", "Active", "true", "boolean")

    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId)
    utils.createFile(fileThreeId, consignmentId)

    runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())

    utils.getFileStatusResult(fileOneId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileOneId, ClosureMetadata).head should equal(Completed)
    utils.getFileStatusResult(fileTwoId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileTwoId, ClosureMetadata).head should equal(Completed)
    utils.getFileStatusResult(fileThreeId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileThreeId, ClosureMetadata).head should equal(Completed)
    utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(NotEntered)
    utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Completed)
  }

  "addOrUpdateBulkFileMetadata" should "create a metadata consignment status of Incomplete if there are existing Incomplete file statuses and an Incomplete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val mandatoryClosure = "MandatoryClosure"
      val optionalMetadata = "OptionalMetadata"
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.randomUUID()

      utils.createFileProperty("TestProperty1", "", "Defined", "text", false, false, mandatoryClosure, "")
      utils.createFileProperty("TestProperty2", "", "Defined", "text", false, false, mandatoryClosure, "")
      utils.createFilePropertyValues("TestProperty1", "value1", false, 1, 2)
      utils.createFilePropertyDependencies(1, "TestProperty2", "")

      utils.createFileProperty("TestProperty3", "", "Defined", "text", false, false, optionalMetadata, "")
      utils.createFileProperty("TestProperty4", "", "Defined", "text", false, false, optionalMetadata, "")
      utils.createFilePropertyValues("TestProperty3", "value2", false, 3, 4)
      utils.createFilePropertyDependencies(3, "TestProperty4", "")

      utils.createFile(fileOneId, consignmentId)
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, DescriptiveMetadata, Incomplete)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, ClosureMetadata, Incomplete)

      runAddOrUpdateBulkFileMetadataTestMutation("mutation_missing_property", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Incomplete)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Incomplete)
  }

  "addOrUpdateBulkFileMetadata" should "create a metadata consignment status of Incomplete if there are existing Incomplete file statuses and a Complete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file
      val closurePropertyGroup = "MandatoryClosure"
      val descriptivePropertyGroup = "OptionalMetadata"

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.randomUUID()

      utils.addFileProperty("newProperty1", closurePropertyGroup)
      utils.addFileProperty("existingPropertyUpdated1", descriptivePropertyGroup)

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFile(fileFourId, consignmentId)

      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, DescriptiveMetadata, Incomplete)
      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, ClosureMetadata, Incomplete)

      runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Incomplete)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Incomplete)
  }

  "addOrUpdateBulkFileMetadata" should "create a metadata consignment status of Complete if there are no existing Incomplete file statuses and a Complete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file
      val closurePropertyGroup = "MandatoryClosure"
      val descriptivePropertyGroup = "OptionalMetadata"

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.randomUUID()

      utils.addFileProperty("newProperty1", closurePropertyGroup)
      utils.addFileProperty("existingPropertyUpdated1", descriptivePropertyGroup)

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFile(fileFourId, consignmentId)

      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, DescriptiveMetadata, Completed)
      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, ClosureMetadata, Completed)

      runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Completed)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Completed)
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

  "addOrUpdateBulkFileMetadata" should "add or update all file metadata when consignment belongs to another user when called by backend checks client" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      utils.addFileProperty("newProperty1")
      utils.addFileProperty("existingPropertyUpdated1")

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyUpdated1", "existingValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyUpdated1", "newValue1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "existingValue1")

      val exportAccessToken = validBackendChecksToken("export")

      val expectedResponse: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        expectedAddOrUpdateBulkFileMetadataMutationResponse("data_all")
      val expectedResponseFileMetadata = expectedResponse.data.get
      val response: GraphqlAddOrUpdateBulkFileMetadataMutationData =
        runAddOrUpdateBulkFileMetadataTestMutation("mutation_alldata", exportAccessToken)
      val responseFileMetadataProperties = response.data.get

      responseFileMetadataProperties should equal(expectedResponseFileMetadata)
  }

  "addOrUpdateBulkFileMetadata" should "throw a 'not authorised' exception is thrown when the user doesn't own the file and the backend checks client is not used" in withContainers {
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

  "updateBulkFileMetadata" should "update all file metadata based on input for non-directories only" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
    utils.addFileProperty("newProperty1")
    utils.addFileProperty("existingPropertyUpdated1")
    utils.addFileProperty("existingPropertyNotUpdated1")

    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId)
    utils.createFile(fileThreeId, consignmentId)
    utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "existingPropertyNotUpdated1", "existingValue1")
    utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "existingPropertyNotUpdated1", "existingValue1")
    utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "newProperty1", "value1")
    utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyUpdated1", "newValue1")
    utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId.toString, "existingPropertyNotUpdated1", "existingValue1")

    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
      expectedUpdateBulkFileMetadataMutationResponse("data_all")
    val expectedResponseFileIds = expectedResponse.data.get.updateBulkFileMetadata.fileIds
    val expectedResponseFileMetadata = expectedResponse.data.get.updateBulkFileMetadata.metadataProperties
    val response: GraphqlUpdateBulkFileMetadataMutationData =
      runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
    val responseFileIds: Seq[UUID] = response.data.get.updateBulkFileMetadata.fileIds
    val responseFileMetadataProperties = response.data.get.updateBulkFileMetadata.metadataProperties

    val correctPropertiesWerePassedIn: Boolean = responseFileMetadataProperties.forall(fileMetadata => expectedResponseFileMetadata.contains(fileMetadata))

    correctPropertiesWerePassedIn should equal(true)
    responseFileIds.sorted should equal(expectedResponseFileIds.sorted)
    responseFileIds.foreach(fileId => responseFileMetadataProperties.foreach(fileMetadata => checkFileMetadataExists(fileId, utils, fileMetadata.filePropertyName)))

    responseFileIds.foreach { id =>
      checkFileMetadataValue(id, utils, "newProperty1", "value1")
      checkFileMetadataValue(id, utils, "existingPropertyUpdated1", "newValue1")
      checkFileMetadataValue(id, utils, "existingPropertyNotUpdated1", "existingValue1")
    }
  }

  "updateBulkFileMetadata" should "set the expected consignment and file statuses for non-directories only" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()
    val propertyGroup = "MandatoryClosure"

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
    utils.addFileProperty("ClosureType", propertyGroup)
    utils.addFileProperty("newProperty1", propertyGroup)
    utils.addFileProperty("existingPropertyUpdated1", propertyGroup)
    utils.addFileProperty("existingPropertyNotUpdated1", propertyGroup)

    utils.createDisplayProperty("newProperty1", "Active", "true", "boolean")
    utils.createDisplayProperty("existingPropertyUpdated1", "Active", "true", "boolean")
    utils.createDisplayProperty("existingPropertyNotUpdated1", "Active", "true", "boolean")

    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId)
    utils.createFile(fileThreeId, consignmentId)

    runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())

    utils.getFileStatusResult(fileOneId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileOneId, ClosureMetadata).head should equal(Completed)
    utils.getFileStatusResult(fileTwoId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileTwoId, ClosureMetadata).head should equal(Completed)
    utils.getFileStatusResult(fileThreeId, DescriptiveMetadata).head should equal(NotEntered)
    utils.getFileStatusResult(fileThreeId, ClosureMetadata).head should equal(Completed)
    utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(NotEntered)
    utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Completed)
  }

  "updateBulkFileMetadata" should "create a metadata consignment status of Incomplete if there are existing Incomplete file statuses and an Incomplete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val mandatoryClosure = "MandatoryClosure"
      val optionalMetadata = "OptionalMetadata"
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries()
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.randomUUID()

      utils.createFileProperty("TestProperty1", "", "Defined", "text", false, false, mandatoryClosure, "")
      utils.createFileProperty("TestProperty2", "", "Defined", "text", false, false, mandatoryClosure, "")
      utils.createFilePropertyValues("TestProperty1", "value1", false, 1, 2)
      utils.createFilePropertyDependencies(1, "TestProperty2", "")

      utils.createFileProperty("TestProperty3", "", "Defined", "text", false, false, optionalMetadata, "")
      utils.createFileProperty("TestProperty4", "", "Defined", "text", false, false, optionalMetadata, "")
      utils.createFilePropertyValues("TestProperty3", "value2", false, 3, 4)
      utils.createFilePropertyDependencies(3, "TestProperty4", "")

      utils.createFile(fileOneId, consignmentId)
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, DescriptiveMetadata, Incomplete)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, ClosureMetadata, Incomplete)

      runUpdateBulkFileMetadataTestMutation("mutation_missing_property", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Incomplete)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Incomplete)
  }

  "updateBulkFileMetadata" should "create a metadata consignment status of Incomplete if there are existing Incomplete file statuses and a Complete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file
      val closurePropertyGroup = "MandatoryClosure"
      val descriptivePropertyGroup = "OptionalMetadata"

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.randomUUID()

      utils.addFileProperty("newProperty1", closurePropertyGroup)
      utils.addFileProperty("existingPropertyUpdated1", descriptivePropertyGroup)

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFile(fileFourId, consignmentId)

      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, DescriptiveMetadata, Incomplete)
      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, ClosureMetadata, Incomplete)

      runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Incomplete)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Incomplete)
  }

  "updateBulkFileMetadata" should "create a metadata consignment status of Complete if there are no existing Incomplete file statuses and a Complete update" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val (consignmentId, _) = utils.seedDatabaseWithDefaultEntries() // this method adds a default file
      val closurePropertyGroup = "MandatoryClosure"
      val descriptivePropertyGroup = "OptionalMetadata"

      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")
      val fileFourId = UUID.randomUUID()

      utils.addFileProperty("newProperty1", closurePropertyGroup)
      utils.addFileProperty("existingPropertyUpdated1", descriptivePropertyGroup)

      utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
      utils.createFile(fileTwoId, consignmentId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFile(fileFourId, consignmentId)

      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, DescriptiveMetadata, Completed)
      utils.createFileStatusValues(UUID.randomUUID(), fileFourId, ClosureMetadata, Completed)

      runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())
      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(Completed)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(Completed)
  }

  "updateBulkFileMetadata" should "not allow bulk updating of non-directories file metadata with incorrect authorisation" in withContainers { case container: PostgreSQLContainer =>
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

    val response: GraphqlUpdateBulkFileMetadataMutationData = runUpdateBulkFileMetadataTestMutation("mutation_alldata", token)

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw an error if the field fileIds is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData = expectedUpdateBulkFileMetadataMutationResponse("data_fileids_missing")
    val response: GraphqlUpdateBulkFileMetadataMutationData = runUpdateBulkFileMetadataTestMutation("mutation_missingfileids", validUserToken())

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw an error if the field metadataProperties is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
      expectedUpdateBulkFileMetadataMutationResponse("data_metadataproperties_missing")
    val response: GraphqlUpdateBulkFileMetadataMutationData =
      runUpdateBulkFileMetadataTestMutation("mutation_missingmetadataproperties", validUserToken())

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw an 'invalid input data' error if some file ids do not exist" in withContainers { case container: PostgreSQLContainer =>
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

    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData = expectedUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
    val response: GraphqlUpdateBulkFileMetadataMutationData =
      runUpdateBulkFileMetadataTestMutation("mutation_fileidnotexists", validUserToken())

    response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw an 'invalid input data' error if the consignment input id does not match the files' consignment id" in withContainers {
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

      val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData = expectedUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlUpdateBulkFileMetadataMutationData =
        runUpdateBulkFileMetadataTestMutation("mutation_different_consignmentid", validUserToken())

      response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw an 'invalid input data' error if input file ids belong to different consignments" in withContainers {
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

      val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
        expectedUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
      val response: GraphqlUpdateBulkFileMetadataMutationData =
        runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())

      response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw a 'not authorised' error if a file id exists but belongs to another user" in withContainers { case container: PostgreSQLContainer =>
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

    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
      expectedUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
    val response: GraphqlUpdateBulkFileMetadataMutationData = runUpdateBulkFileMetadataTestMutation("mutation_notfileowner", validUserToken())

    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFileMetadataAdded(utils, "property1")
    checkNoFileMetadataAdded(utils, "property2")
  }

  "updateBulkFileMetadata" should "throw a 'not authorised' error if a file id belongs to another user and an input error is also present" in withContainers {
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

      val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
        expectedUpdateBulkFileMetadataMutationResponse("data_error_not_file_owner")
      val response: GraphqlUpdateBulkFileMetadataMutationData = runUpdateBulkFileMetadataTestMutation("mutation_notfileowner", validUserToken())

      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoFileMetadataAdded(utils, "property1")
      checkNoFileMetadataAdded(utils, "property2")
  }

  "deleteFileMetadata" should "delete file metadata or set the relevant default values for the given fileIds" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("57a0b4cd-4dc1-4be4-a8c1-2d93fce54413")
    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    addDummyFileProperties(utils, consignmentId, userId)
    createFileAndFileMetadata(utils, consignmentId, folderOneId, fileOneId, fileTwoId)

    val expectedResponse: GraphqlDeleteFileMetadataMutationData = expectedDeleteFileMetadataMutationResponse("data_all")
    val expectedResponseFileIds = expectedResponse.data.get.deleteFileMetadata.fileIds
    val expectedResponseFileMetadata = expectedResponse.data.get.deleteFileMetadata.filePropertyNames
    val response = runDeleteFileMetadataTestMutation("mutation_alldata", validUserToken())
    val responseFileIds: Seq[UUID] = response.data.get.deleteFileMetadata.fileIds
    val responseFileMetadataProperties = response.data.get.deleteFileMetadata.filePropertyNames

    responseFileMetadataProperties.size should equal(expectedResponseFileMetadata.size)
    responseFileMetadataProperties should equal(expectedResponseFileMetadata)

    responseFileIds.sorted should equal(expectedResponseFileIds.sorted)

    responseFileIds.foreach { id =>
      checkFileMetadataDoesNotExist(id, utils, "TestDependency2")
      checkFileMetadataDoesNotExist(id, utils, "TestDependency4")
      checkFileMetadataValue(id, utils, "TestDependency1", "test")
      checkFileMetadataValue(id, utils, "ClosureType", "Open")
    }
  }

  val statusValues: TableFor1[String] = Table(
    "statusValues",
    Incomplete,
    Completed,
    NotEntered
  )

  forAll(statusValues)(statusValue => {
    "deleteFileMetadata" should s"set the consignment status to $statusValue if an existing file is $statusValue" in withContainers { case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("57a0b4cd-4dc1-4be4-a8c1-2d93fce54413")
      val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
      val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
      val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
      val fileThreeId = UUID.randomUUID()
      addDummyFileProperties(utils, consignmentId, userId)
      createFileAndFileMetadata(utils, consignmentId, folderOneId, fileOneId, fileTwoId)
      utils.createFile(fileThreeId, consignmentId)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, DescriptiveMetadata, statusValue)
      utils.createFileStatusValues(UUID.randomUUID(), fileThreeId, ClosureMetadata, statusValue)

      runDeleteFileMetadataTestMutation("mutation_alldata", validUserToken())

      utils.getConsignmentStatus(consignmentId, DescriptiveMetadata).getString("Value") should equal(statusValue)
      utils.getConsignmentStatus(consignmentId, ClosureMetadata).getString("Value") should equal(statusValue)
    }

  })

  "deleteFileMetadata" should "throw an error if the field fileIds is not provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.randomUUID()
    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    addDummyFileProperties(utils, consignmentId, userId)
    createFileAndFileMetadata(utils, consignmentId, folderOneId, fileOneId, fileTwoId)

    val expectedResponse: GraphqlDeleteFileMetadataMutationData = expectedDeleteFileMetadataMutationResponse("data_missing_fileids")
    val response = runDeleteFileMetadataTestMutation("mutation_missing_fileids", validUserToken(userId))

    response.errors.head.message should equal(expectedResponse.errors.head.message)

    List(fileOneId, fileTwoId).foreach(id => {
      checkFileMetadataExists(id, utils, "TestDependency2")
      checkFileMetadataValue(id, utils, "TestDependency1", "newValue")
      checkFileMetadataValue(id, utils, "ClosureType", "Closed")
    })
  }

  "deleteFileMetadata" should "throw an error if the field fileIds is empty" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("57a0b4cd-4dc1-4be4-a8c1-2d93fce54413")
    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    addDummyFileProperties(utils, consignmentId, userId)
    createFileAndFileMetadata(utils, consignmentId, folderOneId, fileOneId, fileTwoId)

    val expectedResponse: GraphqlDeleteFileMetadataMutationData = expectedDeleteFileMetadataMutationResponse("data_empty_fileids")
    val response = runDeleteFileMetadataTestMutation("mutation_empty_fileids", validUserToken(userId))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")

    List(fileOneId, fileTwoId).foreach(id => {
      checkFileMetadataExists(id, utils, "TestDependency2")
      checkFileMetadataValue(id, utils, "TestDependency1", "newValue")
      checkFileMetadataValue(id, utils, "ClosureType", "Closed")
    })
  }

  "deleteFileMetadata" should "throw an error if a file id exists but belongs to another user" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.randomUUID()
    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
    addDummyFileProperties(utils, consignmentId, userId)
    createFileAndFileMetadata(utils, consignmentId, folderOneId, fileOneId, fileTwoId)

    val expectedResponse: GraphqlDeleteFileMetadataMutationData = expectedDeleteFileMetadataMutationResponse("data_error_not_file_owner")
    val response = runDeleteFileMetadataTestMutation("mutation_alldata", validUserToken(wrongUserId))

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")

    List(fileOneId, fileTwoId).foreach(id => {
      checkFileMetadataExists(id, utils, "TestDependency2")
      checkFileMetadataValue(id, utils, "TestDependency1", "newValue")
      checkFileMetadataValue(id, utils, "ClosureType", "Closed")
    })
  }

  "deleteFileMetadata" should "throw an 'invalid input data' error if input file ids belong to different consignments" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("57a0b4cd-4dc1-4be4-a8c1-2d93fce54413")

    val differentConsignmentId = UUID.fromString("7ece007d-6f47-4be2-895f-511a607e4074")
    utils.createConsignment(differentConsignmentId)

    val folderOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileOneId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileTwoId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")
    val fileThreeId = UUID.fromString("d2e64eed-faff-45ac-9825-79548f681323")

    addDummyFileProperties(utils, consignmentId, userId)

    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, differentConsignmentId)
    utils.createFile(fileThreeId, consignmentId)

    List(fileOneId, fileTwoId).foreach(id => {
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "ClosureType", "Closed")
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "TestDependency1", "newValue")
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "TestDependency2", "someValue")
    })

    val expectedResponse: GraphqlUpdateBulkFileMetadataMutationData =
      expectedUpdateBulkFileMetadataMutationResponse("data_fileid_not_exists")
    val response: GraphqlUpdateBulkFileMetadataMutationData =
      runUpdateBulkFileMetadataTestMutation("mutation_alldata", validUserToken())

    response.errors.head.extensions.get.code should equal("INVALID_INPUT_DATA")
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    List(fileOneId, fileTwoId).foreach(id => {
      checkFileMetadataExists(id, utils, "TestDependency2")
      checkFileMetadataValue(id, utils, "TestDependency1", "newValue")
      checkFileMetadataValue(id, utils, "ClosureType", "Closed")
    })
  }

  private def getParentId(fileId: UUID, utils: TestUtils): String = {
    val sql = """SELECT * FROM "File" WHERE "FileId" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ParentId")
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

  private def checkFileMetadataDoesNotExist(fileId: UUID, utils: TestUtils, propertyName: String = SHA256ServerSideChecksum): Unit = {
    val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, propertyName)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
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

  private def createFileAndFileMetadata(utils: TestUtils, consignmentId: UUID, folderOneId: UUID, fileOneId: UUID, fileTwoId: UUID): Unit = {
    utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
    utils.createFile(fileOneId, consignmentId, NodeType.fileTypeIdentifier, "fileName", Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId, NodeType.fileTypeIdentifier)
    List(fileOneId, fileTwoId).foreach(id => {
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "ClosureType", "Closed")
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "TestDependency1", "newValue")
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, "TestDependency2", "someValue")
    })
  }

  private def addDummyFileProperties(utils: TestUtils, consignmentId: UUID, userId: UUID, uiOrdinal: Option[Int] = None): Unit = {
    utils.createConsignment(consignmentId, userId)
    utils.createFileProperty("ClosureType", "It's the Test Property", "Defined", "text", editable = false, multivalue = false, "Test Property Group", "Test Property")

    utils.createFileProperty(
      "TestDependency1",
      "It's the Test Dependency",
      "Defined",
      "text",
      editable = false,
      multivalue = false,
      "MandatoryClosure",
      "Test Dependency",
      2,
      allowExport = true
    )

    utils.createFileProperty(
      "TestDependency2",
      "It's the Test Dependency2",
      "Defined",
      "boolean",
      editable = false,
      multivalue = false,
      "Test Dependency Group",
      "Test Dependency2",
      2,
      allowExport = true
    )

    utils.createFileProperty(
      "TestDependency4",
      "It's the Test Dependency4",
      "Defined",
      "text",
      editable = false,
      multivalue = false,
      "Test Dependency Group",
      "Test Dependency4",
      2,
      allowExport = true
    )

    utils.createFilePropertyValues("ClosureType", "Closed", default = false, 3, 1, uiOrdinal)
    utils.createFilePropertyValues("ClosureType", "Open", default = true, 4, 1, uiOrdinal)
    utils.createFilePropertyValues("TestDependency1", "test", default = true, 0, 1, uiOrdinal)
    utils.createFilePropertyDependencies(3, "TestDependency1", "TestDependencyValue")
    utils.createFilePropertyDependencies(3, "TestDependency2", "TestDependencyValue")
    utils.createFilePropertyDependencies(4, "TestDependency4", "TestDependencyValue")
  }
}
