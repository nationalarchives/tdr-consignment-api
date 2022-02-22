package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.implicits.catsSyntaxOptionId
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.SHA256ServerSideChecksum
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils.{fixedBodyId, fixedSeriesId}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils._

import java.sql.{DriverManager, PreparedStatement, Timestamp, Types}
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.UUID

//scalastyle:off number.of.methods
class ConsignmentRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  private val addConsignmentJsonFilePrefix: String = "json/addconsignment_"
  private val getConsignmentJsonFilePrefix: String = "json/getconsignment_"
  private val consignmentsJsonFilePrefix: String = "json/consignments_"
  private val startUploadJsonFilePrefix: String = "json/startupload_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[GetConsignment], errors: List[GraphqlError] = Nil)

  case class GraphqlConsignmentsQueryData(data: Option[ConsignmentConnections], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationData(data: Option[AddConsignment], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationStartUpload(data: Option[StartUpload], errors: List[GraphqlError] = Nil)

  case class GraphqlMutationExportLocation(data: Option[UpdateExportLocation])

  case class GraphqlMutationTransferInitiated(data: Option[UpdateTransferInitiated])

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
                         currentStatus: Option[CurrentStatus] = None,
                         consignmentType: Option[String],
                         bodyId: Option[UUID] = None,
                         emptyFolders: List[String] = Nil
                        )

  case class PageInfo(startCursor: Option[String] = None, endCursor: Option[String] = None, hasNextPage: Boolean, hasPreviousPage: Boolean)

  case class ConsignmentEdge(node: Consignment, cursor: Option[String] = None)

  case class Consignments(pageInfo: PageInfo, edges: List[ConsignmentEdge])

  case class ConsignmentConnections(consignments: Consignments)

  case class FileChecks(antivirusProgress: Option[AntivirusProgress], checksumProgress: Option[ChecksumProgress], ffidProgress: Option[FfidProgress])

  case class AntivirusProgress(filesProcessed: Option[Int])

  case class ChecksumProgress(filesProcessed: Option[Int])

  case class FfidProgress(filesProcessed: Option[Int])

  case class Series(seriesid: Option[UUID], bodyid: Option[UUID], name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)

  case class TransferringBody(name: Option[String], tdrCode: Option[String])

  case class GetConsignment(getConsignment: Option[Consignment])

  case class AddConsignment(addConsignment: Consignment)

  case class UpdateExportLocation(updateExportLocation: Int)

  case class UpdateTransferInitiated(updateTransferInitiated: Int)

  case class File(fileId: UUID,
                  fileType: Option[String],
                  fileName: Option[String],
                  parentId: Option[UUID],
                  metadata: FileMetadataValues,
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

  val runTestQuery: (String, OAuth2BearerToken, Config) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val runConsignmentsTestQuery: (String, OAuth2BearerToken, Config) =>
    GraphqlConsignmentsQueryData = runTestRequest[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken, Config) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addConsignmentJsonFilePrefix)
  val runTestStartUploadMutation: (String, OAuth2BearerToken, Config) => GraphqlMutationStartUpload =
    runTestRequest[GraphqlMutationStartUpload](startUploadJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val expectedConsignmentsQueryResponse: String =>
    GraphqlConsignmentsQueryData = getDataFromFile[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addConsignmentJsonFilePrefix)

  "addConsignment" should "create a consignment of type 'standard' when standard consignment type provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils: DatabaseUtils = databaseUtils(container)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "default-transferring-body-code"), config(container))
      response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

      checkConsignmentExists(response.data.get.addConsignment.consignmentid.get, utils)
  }

  "addConsignment" should "create a consignment of type 'judgment' when judgment consignment type provided and the user is a judgment user" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_judgment_consignment_type")
      val response: GraphqlMutationData = runTestMutation("mutation_judgment_consignment_type",
        validJudgmentUserToken(body = "default-transferring-body-code"),
        config(container))
      response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

      checkConsignmentExists(response.data.get.addConsignment.consignmentid.get, databaseUtils(container))
  }

  "addConsignment" should "throw an error if no series id and the user is not a 'judgment' user" in withContainers { case container: PostgreSQLContainer =>
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_seriesid")
    val testConfig = config(container)
    val response: GraphqlMutationData = runTestMutation("mutation_no_seriesid", validUserToken(), testConfig)
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if no series id, the user is a judgment user but the consignment type is not 'judgment'" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_seriesid_standard_consignment_type")
      val response: GraphqlMutationData = runTestMutation(
        "mutation_no_seriesid_standard_consignment_type", validJudgmentUserToken(body = "default-transferring-body-code"), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if 'standard' user's body is not the same as the series body" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_incorrect_body")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "incorrect"), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if an invalid consignment type is provided" in withContainers { case container: PostgreSQLContainer =>
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_invalid_consignment_type")
    val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignment_type",
      validUserToken(body = "default-transferring-body-code"), config(container))
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "link a new consignment to the creating user" in withContainers { case container: PostgreSQLContainer =>
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "default-transferring-body-code"), config(container))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    response.data.get.addConsignment.userid should contain(userId)
  }

  "addConsignment" should "not allow a user to link a consignment to a series from another transferring body" in withContainers {
    case container: PostgreSQLContainer =>
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "some-other-transferring-body"), config(container))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  //scalastyle:off magic.number

  "getConsignment" should "return all requested fields" in withContainers { case container: PostgreSQLContainer =>
    val utils = databaseUtils(container)
    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

    val sql =
      """INSERT INTO "Consignment" """ +
        """("ConsignmentId", "SeriesId", "UserId", "Datetime", "TransferInitiatedDatetime",""" +
        """ "ExportDatetime", "ConsignmentReference", "ConsignmentType", "BodyId", "ConsignmentSequence")""" +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = connection.prepareStatement(sql)

    val bodyId = UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8")
    val bodyCode = "consignment-body-code"
    utils.addTransferringBody(bodyId, "Some department name", bodyCode)

    val consignmentId = "b130e097-2edc-4e67-a7e9-5364a09ae9cb"

    val seriesId = "fde450c9-09aa-4ba8-b0df-13f9bac1e587"
    val seriesName = "Mock series"
    utils.addSeries(UUID.fromString(seriesId), bodyId, seriesName)

    val fixedTimeStamp = Timestamp.from(FixedTimeSource.now)
    ps.setObject(1, consignmentId, Types.OTHER)
    ps.setObject(2, seriesId, Types.OTHER)
    ps.setObject(3, userId.toString, Types.OTHER)
    ps.setTimestamp(4, fixedTimeStamp)
    ps.setTimestamp(5, fixedTimeStamp)
    ps.setTimestamp(6, fixedTimeStamp)
    ps.setString(7, "TEST-TDR-2021-MTB")
    ps.setString(8, "standard")
    ps.setObject(9, bodyId, Types.OTHER)
    ps.setInt(10, 1)
    ps.executeUpdate()
    val parentId = "7b19b272-d4d1-4d77-bf25-511dc6489d12"
    val fileOneId = "e7ba59c9-5b8b-4029-9f27-2d03957463ad"
    val fileTwoId = "42910a85-85c3-40c3-888f-32f697bfadb6"
    val fileThreeId = "9757f402-ee1a-43a2-ae2a-81a9ea9729b9"

    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"


    utils.createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentId), fileName = "fileOneName", parentId = UUID.fromString(parentId).some)
    utils.createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentId), fileName = "fileTwoName", parentId = UUID.fromString(parentId).some)
    utils.createFile(UUID.fromString(fileThreeId), UUID.fromString(consignmentId), fileName = "fileThreeName", parentId = UUID.fromString(parentId).some)

    utils.addAntivirusMetadata(fileOneId)

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
    utils.addFileProperty(SHA256ServerSideChecksum)
    utils.addFileMetadata("06209e0d-95d0-4f13-8933-e5b9d00eb435", fileOneId, SHA256ServerSideChecksum)
    utils.addFileMetadata("c4759aae-dc68-45ec-aee1-5a562c7b42cc", fileTwoId, SHA256ServerSideChecksum)

    val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
    utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    utils.addParentFolderName(UUID.fromString(consignmentId), "ALL CONSIGNMENT DATA PARENT FOLDER")

    utils.createConsignmentStatus(UUID.fromString(consignmentId), "Upload", "Completed")

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(body = bodyCode), config(container))

    response should equal(expectedResponse)
  }
  //scalastyle:off magic.number

  "getConsignment" should "return the file metadata" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
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
      clientSideProperties.foreach { csp =>
        utils.addFileProperty(csp)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
          csp match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$csp value"
          }
        )
      }

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken(), config(container))
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata")

      response should equal(expectedResponse)
  }

  "getConsignment" should "return empty ffid metadata if the ffid metadata is missing" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
      val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

      utils.createConsignment(consignmentId, userId, fixedSeriesId)
      utils.createFile(fileId, consignmentId)
      staticMetadataProperties.foreach(smp => {
        utils.addFileProperty(smp.name)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
      })
      clientSideProperties.foreach { csp =>
        utils.addFileProperty(csp)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
          csp match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$csp value"
          }
        )
      }

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken(), config(container))

      response.data.get.getConsignment.get.files.get.head.ffidMetadata.isEmpty should be(true)
  }

  "getConsignment" should "return multiple droid matches" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
      val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

      utils.createConsignment(consignmentId, userId, fixedSeriesId)
      utils.createFile(fileId, consignmentId)
      staticMetadataProperties.foreach(smp => {
        utils.addFileProperty(smp.name)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
      })
      clientSideProperties.foreach { csp =>
        utils.addFileProperty(csp)
        utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
          csp match {
            case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
            case ClientSideFileSize => "1"
            case _ => s"$csp value"
          }
        )
      }

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext1", "identification1", "puid1")
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext2", "identification2", "puid2")

      val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken(), config(container))

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata_multiple_matches")

      response should equal(expectedResponse)
  }

  "getConsignment" should "return the expected data" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
      val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken(), config(container))
      response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "allow a user with export access to return data" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val exportAccessToken = validBackendChecksToken("export")
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
      val response: GraphqlQueryData = runTestQuery("query_somedata", exportAccessToken, config(container))
      response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "not allow a user to get a consignment that they did not create" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = UUID.fromString("f1dbc692-e56c-4d76-be94-d8d3d79bd38a")
      val otherUserId = "73abd1dc-294d-4068-b60d-c1cd4782d08d"
      utils.createConsignment(consignmentId, UUID.fromString(otherUserId))

      val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken(), config(container))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "getConsignment" should "return an error if a user queries without a consignment id argument" in withContainers {
    case container: PostgreSQLContainer =>
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_error_no_consignmentid")
      val response: GraphqlQueryData = runTestQuery("query_no_consignmentid", validUserToken(), config(container))
      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateExportLocation" should "update the export location correctly" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)
      val prefix = "json/updateexportlocation_"
      val expectedResponse = getDataFromFile[GraphqlMutationExportLocation](prefix)("data_all")
      val token = validBackendChecksToken("export")
      val response: GraphqlMutationExportLocation = runTestRequest[GraphqlMutationExportLocation](prefix)("mutation_all", token, config(container))
      response.data should equal(expectedResponse.data)
      val fieldId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      getConsignmentField(fieldId, "ExportLocation", databaseUtils(container)) should equal("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e.tar.gz")
  }

  "updateTransferInitiated" should "update the transfer initiated date correctly" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      utils.createConsignment(new FixedUUIDSource().uuid, userId)
      val prefix = "json/updatetransferinitiated_"
      val expectedResponse = getDataFromFile[GraphqlMutationTransferInitiated](prefix)("data_all")
      val response: GraphqlMutationTransferInitiated =
        runTestRequest[GraphqlMutationTransferInitiated](prefix)("mutation_all", validUserToken(), config(container))
      response.data should equal(expectedResponse.data)
      val field = getConsignmentField(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"), _, databaseUtils(container))
      Option(field("TransferInitiatedDatetime")).isDefined should equal(true)
      field("TransferInitiatedBy") should equal(userId.toString)
  }

  "consignments" should "allow a user with reporting access to return consignments in a paginated format" in withContainers {
    case container: PostgreSQLContainer =>
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

      setUpConsignments(consignmentParams, databaseUtils(container))

      val reportingAccessToken = validReportingToken("reporting")

      val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_all")
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_alldata", reportingAccessToken, config(container))

      response.data.get.consignments should equal(expectedResponse.data.get.consignments)
  }

  "consignments" should "allow a user with reporting access to return requested fields for consignments in a paginated format" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentParams: List[ConsignmentParams] = List(
        ConsignmentParams(UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"), "consignment-ref1", List()),
        ConsignmentParams(UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"), "consignment-ref2", List()),
        ConsignmentParams(UUID.fromString("e6dadac0-0666-4653-b462-adca0b988095"), "consignment-ref3", List())
      )

      setUpConsignments(consignmentParams, databaseUtils(container))
      val reportingAccessToken = validReportingToken("reporting")

      val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_some")
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", reportingAccessToken, config(container))
      response.data.get.consignments.edges.size should equal(2)

      response.data should equal(expectedResponse.data)
  }

  "consignments" should "throw an error if user does not have reporting access" in withContainers {
    case container: PostgreSQLContainer =>
      val exportAccessToken = invalidReportingToken()
      val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", exportAccessToken, config(container))

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "startUpload" should "add the upload status and update the parent folder" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      runTestStartUploadMutation("mutation_alldata", validUserToken(), config(container))

      val consignment = utils.getConsignment(consignmentId)
      consignment.getString("ParentFolder") should equal("parent")
      utils.getConsignmentStatus(consignmentId, "Upload").getString("Value") should equal("InProgress")
  }

  "startUpload" should "return an error if the upload is in progress" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, "Upload", "InProgress")
      val response = runTestStartUploadMutation("mutation_alldata", validUserToken(), config(container))

      response.errors.size should equal(1)
      response.errors.head.message should equal("Existing consignment upload status is 'InProgress', so cannot start new upload")
  }

  "startUpload" should "return an error if the upload is complete" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = databaseUtils(container)
      val consignmentId = new FixedUUIDSource().uuid
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, "Upload", "Complete")
      val response = runTestStartUploadMutation("mutation_alldata", validUserToken(), config(container))

      response.errors.size should equal(1)
      response.errors.head.message should equal("Existing consignment upload status is 'Complete', so cannot start new upload")
  }

  private def checkConsignmentExists(consignmentId: UUID, databaseUtils: DatabaseUtils): Unit = {
    val result = databaseUtils.getConsignment(consignmentId)
    result.getString("ConsignmentId") should equal(consignmentId.toString)
  }

  private def getConsignmentField(consignmentId: UUID, field: String, utils: DatabaseUtils): String = {
    val result = utils.getConsignment(consignmentId)
    result.getString(field)
  }

  private def setUpConsignments(consignmentParams: List[ConsignmentParams], utils: DatabaseUtils): Unit = {
    val seriesId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    utils.addSeries(seriesId, fixedBodyId, "Mock series")
    staticMetadataProperties.foreach(smp => utils.addFileProperty(smp.name))
    clientSideProperties.foreach(csp => utils.addFileProperty(csp))
    utils.addFileProperty(SHA256ServerSideChecksum)

    consignmentParams.foreach(ps => {
      utils.createConsignment(ps.consignmentId, userId, seriesId, consignmentRef = ps.consignmentRef, bodyId = fixedBodyId)
      utils.createConsignmentStatus(ps.consignmentId, "Upload", "Completed")
      utils.addParentFolderName(ps.consignmentId, "ALL CONSIGNMENT DATA PARENT FOLDER")
      ps.fileIds.foreach(fs => {
        setUpFileAndStandardMetadata(ps.consignmentId, fs, utils)
      })
    })
  }

  private def setUpFileAndStandardMetadata(consignmentId: UUID, fileId: UUID, utils: DatabaseUtils): Unit = {
    utils.createFile(fileId, consignmentId)
    utils.createFile(UUID.randomUUID(), consignmentId, NodeType.folderTypeIdentifier)
    generateMetadataPropertiesForFile(fileId, utils)
    utils.addAntivirusMetadata(fileId.toString)
    utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, SHA256ServerSideChecksum)
    setUpStandardFFIDMatchesForFile(fileId, utils)
  }

  private def generateMetadataPropertiesForFile(fileId: UUID, utils: DatabaseUtils): Unit = {
    staticMetadataProperties.foreach(smp => {
      utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value)
    })
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

  private def setUpStandardFFIDMatchesForFile(fileId: UUID, utils: DatabaseUtils): Unit = {
    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"

    val fileFfidMetadataId = utils.addFFIDMetadata(fileId.toString)
    utils.addFFIDMetadataMatches(fileFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)
  }
}

case class ConsignmentParams(consignmentId: UUID, consignmentRef: String, fileIds: List[UUID])
