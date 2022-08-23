package uk.gov.nationalarchives.tdr.api.routes

import java.time.{LocalDateTime, ZonedDateTime}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.implicits.catsSyntaxOptionId
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.SHA256ServerSideChecksum
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}

//scalastyle:off number.of.methods
//scalastyle:off number.of.types
class ConsignmentRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addConsignmentJsonFilePrefix: String = "json/addconsignment_"
  private val getConsignmentJsonFilePrefix: String = "json/getconsignment_"
  private val consignmentsJsonFilePrefix: String = "json/consignments_"
  private val startUploadJsonFilePrefix: String = "json/startupload_"
  private val updateConsignmentSeriesIdJsonFilePrefix: String = "json/updateconsignmentseriesid_"

  val defaultConsignmentId: UUID = UUID.fromString("b130e097-2edc-4e67-a7e9-5364a09ae9cb")
  val parentUUID: Option[UUID] = UUID.fromString("7b19b272-d4d1-4d77-bf25-511dc6489d12").some
  val fileOneId = "e7ba59c9-5b8b-4029-9f27-2d03957463ad"
  val fileTwoId = "42910a85-85c3-40c3-888f-32f697bfadb6"
  val fileThreeId = "9757f402-ee1a-43a2-ae2a-81a9ea9729b9"
  val defaultBodyCode = "default-transferring-body-code"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[GetConsignment], errors: List[GraphqlError] = Nil)

  case class GraphqlConsignmentsQueryData(data: Option[ConsignmentConnections], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationData(data: Option[AddConsignment], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationStartUpload(data: Option[StartUpload], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationExportData(data: Option[UpdateExportData])

  case class GraphqlMutationTransferInitiated(data: Option[UpdateTransferInitiated])

  case class GraphqlMutationUpdateSeriesIdOfConsignment(data: Option[UpdateSeriesIdOfConsignment], errors: List[GraphqlError] = Nil)

  case class Consignment(consignmentid: Option[UUID] = None,
                         userid: Option[UUID] = None,
                         seriesid: Option[UUID] = None,
                         createdDatetime: Option[ZonedDateTime] = None,
                         transferInitiatedDatetime: Option[ZonedDateTime] = None,
                         exportDatetime: Option[ZonedDateTime] = None,
                         totalFiles: Option[Int],
                         fileChecks: Option[FileChecks],
                         parentFolder: Option[String],
                         series: Option[Series],
                         transferringBody: Option[TransferringBody],
                         files: Option[List[File]],
                         paginatedFiles: Option[FileConnections],
                         currentStatus: Option[CurrentStatus] = None,
                         consignmentType: Option[String],
                         bodyId: Option[UUID] = None
                        )

  case class PageInfo(startCursor: Option[String] = None, endCursor: Option[String] = None, hasNextPage: Boolean, hasPreviousPage: Boolean)

  case class ConsignmentEdge(node: Consignment, cursor: Option[String] = None)
  case class FileEdge(node: File, cursor: String)

  case class Consignments(pageInfo: PageInfo, edges: List[ConsignmentEdge])

  case class ConsignmentConnections(consignments: Consignments)
  case class FileConnections(pageInfo: PageInfo, edges: List[FileEdge])

  case class FileChecks(antivirusProgress: Option[AntivirusProgress], checksumProgress: Option[ChecksumProgress], ffidProgress: Option[FfidProgress])

  case class AntivirusProgress(filesProcessed: Option[Int])

  case class ChecksumProgress(filesProcessed: Option[Int])

  case class FfidProgress(filesProcessed: Option[Int])

  case class Series(seriesid: Option[UUID], bodyid: Option[UUID], name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)

  case class TransferringBody(name: Option[String], tdrCode: Option[String])

  case class GetConsignment(getConsignment: Option[Consignment])

  case class AddConsignment(addConsignment: Consignment)

  case class UpdateExportData(updateExportData: Int)

  case class UpdateTransferInitiated(updateTransferInitiated: Int)

  case class UpdateSeriesIdOfConsignment(updateConsignmentSeriesId: Option[Int])

  case class File(fileId: UUID,
                  fileType: Option[String],
                  fileName: Option[String],
                  parentId: Option[UUID],
                  metadata: FileMetadataValues = FileMetadataValues(None, None, None, None, None, None, None, None, None),
                  fileStatus: Option[String],
                  ffidMetadata: Option[FFIDMetadataValues])

  case class FFIDMetadataMatches(extension: Option[String] = None, identificationBasis: String, puid: Option[String])

  case class FileMetadataValues(sha256ClientSideChecksum: Option[String],
                                clientSideOriginalFilePath: Option[String],
                                clientSideLastModifiedDate: Option[LocalDateTime],
                                clientSideFileSize: Option[Long],
                                rightsCopyright: Option[String],
                                legalStatus: Option[String],
                                heldBy: Option[String],
                                language: Option[String],
                                foiExemptionCode: Option[String]
                               )

  case class FFIDMetadataValues(software: String,
                                softwareVersion: String,
                                binarySignatureFileVersion: String,
                                containerSignatureFileVersion: String,
                                method: String,
                                matches: List[FFIDMetadataMatches],
                                datetime: Long)

  case class CurrentStatus(upload: Option[String])

  case class StartUpload(startUpload: String)

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val runConsignmentsTestQuery: (String, OAuth2BearerToken) =>
    GraphqlConsignmentsQueryData = runTestRequest[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addConsignmentJsonFilePrefix)
  val runTestStartUploadMutation: (String, OAuth2BearerToken) => GraphqlMutationStartUpload =
    runTestRequest[GraphqlMutationStartUpload](startUploadJsonFilePrefix)
  val runUpdateConsignmentSeriesIdMutation: (String, OAuth2BearerToken) => GraphqlMutationUpdateSeriesIdOfConsignment =
    runTestRequest[GraphqlMutationUpdateSeriesIdOfConsignment](updateConsignmentSeriesIdJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val expectedConsignmentsQueryResponse: String =>
    GraphqlConsignmentsQueryData = getDataFromFile[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addConsignmentJsonFilePrefix)
  val expectedUpdateConsignmentSeriesIdMutationResponse: String =>
    GraphqlMutationUpdateSeriesIdOfConsignment = getDataFromFile[GraphqlMutationUpdateSeriesIdOfConsignment](updateConsignmentSeriesIdJsonFilePrefix)

  "addConsignment" should "create a consignment of type 'standard' when standard consignment type provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = defaultBodyCode))
      response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

      checkConsignmentExists(response.data.get.addConsignment.consignmentid.get, utils)
  }

  "addConsignment" should "create a consignment of type 'judgment' when judgment consignment type provided and the user is a judgment user" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_judgment_consignment_type")
      val response: GraphqlMutationData = runTestMutation("mutation_judgment_consignment_type", validJudgmentUserToken(body = defaultBodyCode))
      response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

      checkConsignmentExists(response.data.get.addConsignment.consignmentid.get, utils)
  }

  "addConsignment" should "throw an error if an invalid consignment type is provided" in withContainers {
    case _: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_invalid_consignment_type")
      val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignment_type", validUserToken(body = defaultBodyCode))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "link a new consignment to the creating user" in withContainers {
    case _: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = defaultBodyCode))
      response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

      response.data.get.addConsignment.userid should contain(userId)
  }

  //scalastyle:off magic.number
  "getConsignment" should "return all requested fields" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(defaultConsignmentId, userId, fixedSeriesId, "TEST-TDR-2021-MTB")

      val extensionMatch = "txt"
      val identificationBasisMatch = "TEST DATA identification"
      val puidMatch = "TEST DATA puid"

      List(SHA256ServerSideChecksum, ClosurePeriod, FoiExemptionAsserted, TitlePublic, ClosureStartDate).foreach(utils.addFileProperty)

      utils.createFile(UUID.fromString(fileOneId), defaultConsignmentId, fileName = "fileOneName", parentId = parentUUID)
      utils.createFile(UUID.fromString(fileTwoId), defaultConsignmentId, fileName = "fileTwoName", parentId = parentUUID)
      utils.createFile(UUID.fromString(fileThreeId), defaultConsignmentId, fileName = "fileThreeName", parentId = parentUUID)

      utils.createFileStatusValues(UUID.randomUUID(),UUID.fromString(fileOneId),"FFID", "Success")
      utils.createFileStatusValues(UUID.randomUUID(),UUID.fromString(fileTwoId),"FFID", "Success")
      utils.createFileStatusValues(UUID.randomUUID(),UUID.fromString(fileThreeId),"FFID", "Success")

      utils.addAntivirusMetadata(fileOneId)

      utils.addFileMetadata("06209e0d-95d0-4f13-8933-e5b9d00eb435", fileOneId, SHA256ServerSideChecksum)
      utils.addFileMetadata("c4759aae-dc68-45ec-aee1-5a562c7b42cc", fileTwoId, SHA256ServerSideChecksum)
      (clientSideProperties ++ staticMetadataProperties.map(_.name)).foreach(propertyName => {
        utils.addFileProperty(propertyName)
        val value = propertyName match {
          case ClientSideFileLastModifiedDate => "2021-03-11 12:30:30.592853"
          case ClientSideFileSize => "1"
          case _ => s"$propertyName value"
        }
        utils.addFileMetadata(UUID.randomUUID().toString, fileOneId, propertyName, value)
        utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId, propertyName, value)
        utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId, propertyName, value)
      })
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId, ClosureStartDate, "2021-04-11 12:30:30.592853")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId, FoiExemptionAsserted, "2021-03-12 12:30:30.592853")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId, TitlePublic, "true")
      utils.addFileMetadata(UUID.randomUUID().toString, fileThreeId, ClosurePeriod, "1")

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

      val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
      utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

      val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
      utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

      utils.addParentFolderName(defaultConsignmentId, "ALL CONSIGNMENT DATA PARENT FOLDER")

      utils.createConsignmentStatus(defaultConsignmentId, "Upload", "Completed")

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
      val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }
  //scalastyle:off magic.number

  "getConsignment" should "return the file metadata" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
      val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

      val extensionMatch = "txt"
      val identificationBasisMatch = "TEST DATA identification"
      val puidMatch = "TEST DATA puid"

      utils.createConsignment(consignmentId, userId, fixedSeriesId)
      utils.createFile(fileId, consignmentId)
      staticMetadataProperties.foreach(smp => {
        utils.addFileProperty(smp.name)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
      })
      clientSideProperties.foreach { propertyName =>
        utils.addFileProperty(propertyName)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, propertyName,
          propertyName match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$propertyName value"
          }
        )
      }

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata")
      response should equal(expectedResponse)
  }

  "getConsignment" should "return empty ffid metadata if the ffid metadata is missing" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
      val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

      utils.createConsignment(consignmentId, userId, fixedSeriesId)
      utils.createFile(fileId, consignmentId)
      staticMetadataProperties.foreach(smp => {
        utils.addFileProperty(smp.name)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
      })
      clientSideProperties.foreach { propertyName =>
        utils.addFileProperty(propertyName)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, propertyName,
          propertyName match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$propertyName value"
          }
        )
      }

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())

      response.data.get.getConsignment.get.files.get.head.ffidMetadata.isEmpty should be(true)
  }

  "getConsignment" should "return multiple droid matches" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
      val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

      utils.createConsignment(consignmentId, userId, fixedSeriesId)
      utils.createFile(fileId, consignmentId)
      staticMetadataProperties.foreach(smp => {
        utils.addFileProperty(smp.name)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
      })
      clientSideProperties.foreach { propertyName =>
        utils.addFileProperty(propertyName)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, propertyName,
          propertyName match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$propertyName value"
          }
        )
      }

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext1", "identification1", "puid1")
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext2", "identification2", "puid2")

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata_multiple_matches")

      response should equal(expectedResponse)
  }

  "getConsignment" should "return the expected data" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
      val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())
      response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "allow a user with export access to return data" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val exportAccessToken = validBackendChecksToken("export")
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
      val response: GraphqlQueryData = runTestQuery("query_somedata", exportAccessToken)
      response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "not allow a user to get a consignment that they did not create" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("f1dbc692-e56c-4d76-be94-d8d3d79bd38a")
      val otherUserId = "73abd1dc-294d-4068-b60d-c1cd4782d08d"
      utils.createConsignment(consignmentId, UUID.fromString(otherUserId))

      val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "getConsignment" should "return an error if a user queries without a consignment id argument" in withContainers {
    case _: PostgreSQLContainer =>
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_error_no_consignmentid")
      val response: GraphqlQueryData = runTestQuery("query_no_consignmentid", validUserToken())
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "getConsignment" should "return files and directories in the files list" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("e72d94d5-ae79-4a05-bee9-86d9dea2bcc9")
      utils.createConsignment(consignmentId, userId)
      utils.addFileProperty(SHA256ServerSideChecksum)
      staticMetadataProperties.map(_.name).foreach(utils.addFileProperty)
      clientSideProperties.foreach(utils.addFileProperty)
      val topDirectory = UUID.fromString("ce0a51a5-a224-474f-b3a4-df75effd5b34")
      val subDirectory = UUID.fromString("2753ceca-4df3-436b-8891-78ad38e2e8c5")
      val fileId = UUID.fromString("6420152a-aaf2-4401-a309-f67ae35f5702")
      createDirectoryAndMetadata(consignmentId, topDirectory, utils, "directory")
      createDirectoryAndMetadata(consignmentId, subDirectory, utils, "subDirectory", topDirectory.some)
      setUpFileAndStandardMetadata(consignmentId, fileId, utils, subDirectory.some)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_files_and_directories")
      val response: GraphqlQueryData = runTestQuery("query_file_data", validUserToken())
      response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "return all the file edges after the cursor up to the limit value" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_cursor")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_cursor", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return all the file edges up to the limit where no cursor provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_no_cursor")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_no_cursor", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return no file edges where limit set to 0" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_limit_0")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_limit_0", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return all the file edges where non-existent cursor value provided, and filedId is greater than the cursor value" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_non-existent_cursor")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_non-existent_cursor", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should
    "return all the file edges after the cursor to the maximum limit where the requested limit is greater than the maximum" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_limit_greater_max")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_limit_greater_max", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return an error where no 'paginationInput' argument provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_no_input")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_no_input", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return all the file edges in file name alphabetical order, up to the limit where no offset provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_no_offset")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_no_offset", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return all the file edges in file name alphabetical order, using offset up to the limit value" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_offset")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_offset", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return no file edges where offset provided is beyond number of files" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_offset_max")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_offset_max", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return file edges in folder name alphabetical order, up to the limit where a folder Filter is provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_folder_filter")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_folder_filter", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return file edges in file name alphabetical order, up to the limit where a file Filter is provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_file_filter")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_file_filter", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "getConsignment" should "return file edges in file name alphabetical order, up to the limit where a parentId Filter is provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      setUpStandardConsignmentAndFiles(utils)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_paginated_files_parentid_filter")
      val response: GraphqlQueryData = runTestQuery("query_paginated_files_parentid_filter", validUserToken(body = defaultBodyCode))

      response should equal(expectedResponse)
  }

  "updateExportData" should "update the export data correctly" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      val prefix = "json/updateexportdata_"
      val expectedResponse = getDataFromFile[GraphqlMutationExportData](prefix)("data_all")
      val token = validBackendChecksToken("export")
      val response: GraphqlMutationExportData = runTestRequest[GraphqlMutationExportData](prefix)("mutation_all", token)
      response.data should equal(expectedResponse.data)
      val exportLocationField = getConsignmentField(consignmentId, "ExportLocation", utils)
      val exportDatetimeField = getConsignmentField(consignmentId, "ExportDatetime", utils)
      val exportVersionField = getConsignmentField(consignmentId, "ExportVersion", utils)

      exportLocationField should equal("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e.tar.gz")
      exportDatetimeField should equal("2020-01-01 09:00:00+00")
      exportVersionField should equal("0.0.0")
  }

  "updateTransferInitiated" should "update the transfer initiated date correctly" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)
      val prefix = "json/updatetransferinitiated_"
      val expectedResponse = getDataFromFile[GraphqlMutationTransferInitiated](prefix)("data_all")
      val response: GraphqlMutationTransferInitiated = runTestRequest[GraphqlMutationTransferInitiated](prefix)("mutation_all", validUserToken())
      response.data should equal(expectedResponse.data)
      val field = getConsignmentField(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"), _, utils)
      Option(field("TransferInitiatedDatetime")).isDefined should equal(true)
      field("TransferInitiatedBy") should equal(userId.toString)
  }

  "consignments" should "allow a user with reporting access to return consignments in a paginated format" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentParams: List[ConsignmentParams] = List(
        ConsignmentParams(UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"),
          "consignment-ref1",
          List(UUID.fromString("9b003759-a9a2-4bf9-8e34-14079bdaed58"))),
        ConsignmentParams(UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"),
          "consignment-ref2",
          List(UUID.fromString("62c53beb-84d6-4676-80ea-b43f5329de72"))),
        ConsignmentParams(UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a"),
          "consignment-ref3",
          List(UUID.fromString("6f9d3202-aca0-48b6-b464-6c0a2ff61bd8")))
      )
      utils.addFileProperty(SHA256ServerSideChecksum)
      setUpConsignments(consignmentParams, utils)

      val reportingAccessToken = validReportingToken("reporting")

      val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_all")
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_alldata", reportingAccessToken)

      response.data.get.consignments should equal(expectedResponse.data.get.consignments)
  }

  "consignments" should "allow a user with reporting access to return requested fields for consignments in a paginated format" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentParams: List[ConsignmentParams] = List(
        ConsignmentParams(UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"), "consignment-ref1", List()),
        ConsignmentParams(UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"), "consignment-ref2", List()),
        ConsignmentParams(UUID.fromString("e6dadac0-0666-4653-b462-adca0b988095"), "consignment-ref3", List())
      )

      setUpConsignments(consignmentParams, utils)
      val reportingAccessToken = validReportingToken("reporting")

      val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_some")
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", reportingAccessToken)
      response.data.get.consignments.edges.size should equal(2)

      response.data should equal(expectedResponse.data)
  }

  "consignments" should "throw an error if user does not have reporting access" in withContainers {
    case _: PostgreSQLContainer =>
      val exportAccessToken = invalidReportingToken()
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", exportAccessToken)

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "startUpload" should "add the upload status and update the parent folder" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      runTestStartUploadMutation("mutation_alldata", validUserToken())

      val consignment = utils.getConsignment(consignmentId)
      consignment.getString("ParentFolder") should equal("parent")
      utils.getConsignmentStatus(consignmentId, "Upload").getString("Value") should equal("InProgress")
  }

  "startUpload" should "return an error if the upload is in progress" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)

      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, "Upload", "InProgress")
      val response = runTestStartUploadMutation("mutation_alldata", validUserToken())

      response.errors.size should equal(1)
      response.errors.head.message should equal("Existing consignment upload status is 'InProgress', so cannot start new upload")
  }

  "startUpload" should "return an error if the upload is complete" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)

      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, "Upload", "Complete")
      val response = runTestStartUploadMutation("mutation_alldata", validUserToken())

      response.errors.size should equal(1)
      response.errors.head.message should equal("Existing consignment upload status is 'Complete', so cannot start new upload")
  }

  "updateSeriesIdOfConsignment" should "update the consignment with a series id" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)

      val seriesId = "4252c920-b1ac-4b0a-9711-33409b8fae6e"
      utils.addSeries(UUID.fromString(seriesId), fixedBodyId, "MOCK1")

      val expectedResponse: GraphqlMutationUpdateSeriesIdOfConsignment =expectedUpdateConsignmentSeriesIdMutationResponse("data_all")
      val response: GraphqlMutationUpdateSeriesIdOfConsignment =
        runUpdateConsignmentSeriesIdMutation("mutation_all", validUserToken(body = defaultBodyCode))

      response.data.get.updateConsignmentSeriesId should equal(expectedResponse.data.get.updateConsignmentSeriesId)
      val field = getConsignmentField(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"), _, utils)
      field("SeriesId") should equal(seriesId)
  }

  "updateSeriesIdOfConsignment" should "throw an error if 'standard' user's body is not the same as the series body" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)

      val seriesId = "4252c920-b1ac-4b0a-9711-33409b8fae6b"
      utils.addSeries(UUID.fromString(seriesId), fixedBodyId, "MOCK1")

      val expectedResponse: GraphqlMutationUpdateSeriesIdOfConsignment =expectedUpdateConsignmentSeriesIdMutationResponse("data_incorrect_body")
      val response: GraphqlMutationUpdateSeriesIdOfConsignment =
        runUpdateConsignmentSeriesIdMutation("mutation_incorrect_body", validUserToken(body = "incorrect"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  private def checkConsignmentExists(consignmentId: UUID, utils: TestUtils): Unit = {
    val result = utils.getConsignment(consignmentId)
    result.getString("ConsignmentId") should equal(consignmentId.toString)
  }

  private def getConsignmentField(consignmentId: UUID, field: String, utils: TestUtils): String = {
    val result = utils.getConsignment(consignmentId)
    result.getString(field)
  }

  private def setUpConsignments(consignmentParams: List[ConsignmentParams], utils: TestUtils): Unit = {
    (staticMetadataProperties.map(_.name) ++ clientSideProperties).foreach(prop => utils.addFileProperty(prop))

    consignmentParams.foreach(ps => {
      utils.createConsignment(ps.consignmentId, userId, fixedSeriesId, consignmentRef = ps.consignmentRef, bodyId = fixedBodyId)
      utils.createConsignmentStatus(ps.consignmentId, "Upload", "Completed")
      utils.addParentFolderName(ps.consignmentId, "ALL CONSIGNMENT DATA PARENT FOLDER")
      ps.fileIds.foreach(fs => {
        setUpFileAndStandardMetadata(ps.consignmentId, fs, utils)
      })
    })
  }

  private def setUpFileAndStandardMetadata(consignmentId: UUID, fileId: UUID, utils: TestUtils, parentId: Option[UUID] = None): Unit = {
    utils.createFile(fileId, consignmentId, parentId = parentId)
    generateMetadataPropertiesForFile(fileId, utils)
    utils.addAntivirusMetadata(fileId.toString)
    utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, SHA256ServerSideChecksum)
    setUpStandardFFIDMatchesForFile(fileId, utils)
  }

  private def generateMetadataPropertiesForFile(fileId: UUID, utils: TestUtils): Unit = {
    staticMetadataProperties.foreach(smp => utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value))
    clientSideProperties.foreach { csp =>
      utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
        csp match {
          case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
          case ClientSideFileSize => "1"
          case _ => s"$csp value"
        }
      )
    }
  }

  private def createDirectoryAndMetadata(consignmentId: UUID, fileId: UUID, utils: TestUtils, fileName: String, parentId: Option[UUID] = None): UUID = {
    utils.createFile(fileId, consignmentId, NodeType.directoryTypeIdentifier, fileName, parentId = parentId)
    staticMetadataProperties.foreach(p => utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, p.name, p.value))
    utils.addFileMetadata(UUID.randomUUID.toString, fileId.toString, ClientSideOriginalFilepath, fileName)
    fileId
  }

  private def setUpStandardFFIDMatchesForFile(fileId: UUID, utils: TestUtils): Unit = {
    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"

    val fileFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
    utils.addFFIDMetadataMatches(fileFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)
  }

  private def setUpStandardConsignmentAndFiles(utils: TestUtils): Unit = {
    utils.createConsignment(defaultConsignmentId, userId, fixedSeriesId, "TEST-TDR-2021-MTB")
    utils.createFile(parentUUID.get, defaultConsignmentId, NodeType.directoryTypeIdentifier, "parentFolderName")
    utils.createFile(UUID.fromString(fileOneId), defaultConsignmentId, fileName = "fileOneName", parentId = parentUUID)
    utils.createFile(UUID.fromString(fileTwoId), defaultConsignmentId, fileName = "fileTwoName", parentId = parentUUID)
    utils.createFile(UUID.fromString(fileThreeId), defaultConsignmentId, fileName = "fileThreeName", parentId = parentUUID)
    utils.addParentFolderName(defaultConsignmentId, "ALL CONSIGNMENT DATA PARENT FOLDER")
    utils.createConsignmentStatus(defaultConsignmentId, "Upload", "Completed")
  }
}

case class ConsignmentParams(consignmentId: UUID, consignmentRef: String, fileIds: List[UUID])
