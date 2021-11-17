package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, Timestamp}
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.SHA256ServerSideChecksum
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource, TestDatabase, TestRequest}

//scalastyle:off number.of.methods
class ConsignmentRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase {
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
                         bodyId: Option[UUID] = None
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
  case class File(fileId: UUID, metadata: FileMetadataValues, ffidMetadata: Option[FFIDMetadataValues])
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
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val expectedConsignmentsQueryResponse: String =>
    GraphqlConsignmentsQueryData = getDataFromFile[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addConsignmentJsonFilePrefix)

  val fixedSeriesId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val fixedBodyId: UUID = UUID.fromString("830f0315-e683-440e-90d0-5f4aa60388c6")

  override def beforeEach(): Unit = {
    super.beforeEach()

    addTransferringBody(
      fixedBodyId,
      "Default transferring body name",
      "default-transferring-body-code")
  }

  "addConsignment" should "create a consignment of type 'standard' when standard consignment type provided" in {
    addSeries(
      fixedSeriesId,
      fixedBodyId,
      "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "default-transferring-body-code"))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    checkConsignmentExists(response.data.get.addConsignment.consignmentid.get)
  }

  "addConsignment" should "create a consignment of type 'judgment' when judgment consignment type provided and the user is a judgment user" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_judgment_consignment_type")
    val response: GraphqlMutationData = runTestMutation("mutation_judgment_consignment_type", validJudgmentUserToken(body = "default-transferring-body-code"))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    checkConsignmentExists(response.data.get.addConsignment.consignmentid.get)
  }

  "addConsignment" should "throw an error if no series id and the user is not a 'judgment' user" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_seriesid")
    val response: GraphqlMutationData = runTestMutation("mutation_no_seriesid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if no series id, the user is a judgment user but the consignment type is not 'judgment'" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_seriesid_standard_consignment_type")
    val response: GraphqlMutationData = runTestMutation(
      "mutation_no_seriesid_standard_consignment_type", validJudgmentUserToken(body = "default-transferring-body-code"))
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if 'standard' user's body is not the same as the series body" in {
    addSeries(
      fixedSeriesId,
      fixedBodyId,
      "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_incorrect_body")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "incorrect"))
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "throw an error if an invalid consignment type is provided" in {
    addSeries(
      fixedSeriesId,
      fixedBodyId,
      "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_invalid_consignment_type")
    val response: GraphqlMutationData = runTestMutation("mutation_invalid_consignment_type", validUserToken(body = "default-transferring-body-code"))
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "link a new consignment to the creating user" in {
    addSeries(
      fixedSeriesId,
      fixedBodyId,
      "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "default-transferring-body-code"))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    response.data.get.addConsignment.userid should contain(userId)
  }

  "addConsignment" should "not allow a user to link a consignment to a series from another transferring body" in {
    addSeries(
      fixedSeriesId,
      fixedBodyId,
      "Mock series")

    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "some-other-transferring-body"))

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  //scalastyle:off magic.number
  "getConsignment" should "return all requested fields" in {
    val sql = "INSERT INTO Consignment" +
      "(ConsignmentId, SeriesId, UserId, Datetime, TransferInitiatedDatetime," +
      "ExportDatetime, ConsignmentReference, ConsignmentType, BodyId)" +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = databaseConnection.prepareStatement(sql)
    val bodyId = UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8")
    val bodyCode = "consignment-body-code"
    val consignmentId = "b130e097-2edc-4e67-a7e9-5364a09ae9cb"
    val seriesId = "fde450c9-09aa-4ba8-b0df-13f9bac1e587"
    val fixedTimeStamp = Timestamp.from(FixedTimeSource.now)
    ps.setString(1, consignmentId)
    ps.setString(2, seriesId)
    ps.setString(3, userId.toString)
    ps.setTimestamp(4, fixedTimeStamp)
    ps.setTimestamp(5, fixedTimeStamp)
    ps.setTimestamp(6, fixedTimeStamp)
    ps.setString(7, "TEST-TDR-2021-MTB")
    ps.setString(8, "standard")
    ps.setString(9, bodyId.toString)
    ps.executeUpdate()
    val fileOneId = "e7ba59c9-5b8b-4029-9f27-2d03957463ad"
    val fileTwoId = "42910a85-85c3-40c3-888f-32f697bfadb6"
    val fileThreeId = "9757f402-ee1a-43a2-ae2a-81a9ea9729b9"

    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"

    createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentId))
    createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentId))
    createFile(UUID.fromString(fileThreeId), UUID.fromString(consignmentId))

    addAntivirusMetadata(fileOneId)

    addFileMetadata("06209e0d-95d0-4f13-8933-e5b9d00eb435", fileOneId, SHA256ServerSideChecksum)
    addFileMetadata("c4759aae-dc68-45ec-aee1-5a562c7b42cc", fileTwoId, SHA256ServerSideChecksum)
    (clientSideProperties ++ staticMetadataProperties.map(_.name)).foreach(propertyName => {
      val value = propertyName match {
        case ClientSideFileLastModifiedDate => "2021-03-11 12:30:30.592853"
        case ClientSideFileSize => "1"
        case _ => s"$propertyName value"
      }
      addFileMetadata(UUID.randomUUID().toString, fileOneId, propertyName, value)
      addFileMetadata(UUID.randomUUID().toString, fileTwoId, propertyName, value)
      addFileMetadata(UUID.randomUUID().toString, fileThreeId, propertyName, value)
    })

    val fileOneFfidMetadataId = addFFIDMetadata(fileOneId)
    addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    val fileTwoFfidMetadataId = addFFIDMetadata(fileTwoId)
    addFFIDMetadataMatches(fileTwoFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    val fileThreeFfidMetadataId = addFFIDMetadata(fileThreeId)
    addFFIDMetadataMatches(fileThreeFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    addParentFolderName(UUID.fromString(consignmentId), "ALL CONSIGNMENT DATA PARENT FOLDER")

    addTransferringBody(bodyId, "Some department name", bodyCode)

    val seriesName = "Mock series"
    addSeries(UUID.fromString(seriesId), bodyId, seriesName)

    createConsignmentStatus(UUID.fromString(consignmentId), "Upload", "Completed")

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(body = bodyCode))

    response should equal(expectedResponse)
  }
  //scalastyle:off magic.number

  "getConsignment" should "return the file metadata" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"

    createConsignment(consignmentId, userId, UUID.randomUUID())
    createFile(fileId, consignmentId)
    staticMetadataProperties.foreach(smp => addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value))
    clientSideProperties.foreach { csp =>
      addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
        csp match {
          case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
          case ClientSideFileSize => "1"
          case _ => s"$csp value"
        }
      )
    }

    val fileOneFfidMetadataId = addFFIDMetadata(fileId.toString)
    addFFIDMetadataMatches(fileOneFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)

    val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata")

    response should equal(expectedResponse)
  }

  "getConsignment" should "return empty ffid metadata if the ffid metadata is missing" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    createConsignment(consignmentId, userId, UUID.randomUUID())
    createFile(fileId, consignmentId)
    staticMetadataProperties.foreach(smp => addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value))
    clientSideProperties.foreach { csp =>
      addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
        csp match {
          case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
          case ClientSideFileSize => "1"
          case _ => s"$csp value"
        }
      )
    }

    val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())

    response.data.get.getConsignment.get.files.get.head.ffidMetadata.isEmpty should be(true)
  }

  "getConsignment" should "return multiple droid matches" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    createConsignment(consignmentId, userId, UUID.randomUUID())
    createFile(fileId, consignmentId)
    staticMetadataProperties.foreach(smp => addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value))
    clientSideProperties.foreach { csp =>
      addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
        csp match {
          case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
          case ClientSideFileSize => "1"
          case _ => s"$csp value"
        }
      )
    }

    val fileOneFfidMetadataId = addFFIDMetadata(fileId.toString)
    addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext1", "identification1", "puid1")
    addFFIDMetadataMatches(fileOneFfidMetadataId.toString, "ext2", "identification2", "puid2")

    val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata_multiple_matches")

    response should equal(expectedResponse)
  }

  "getConsignment" should "return the expected data" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())
    response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "allow a user with export access to return data" in {
    val exportAccessToken = validBackendChecksToken("export")
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_some")
    val response: GraphqlQueryData = runTestQuery("query_somedata", exportAccessToken)
    response.data should equal(expectedResponse.data)
  }

  "getConsignment" should "not allow a user to get a consignment that they did not create" in {
    val consignmentId = UUID.fromString("f1dbc692-e56c-4d76-be94-d8d3d79bd38a")
    val otherUserId = "73abd1dc-294d-4068-b60d-c1cd4782d08d"
    createConsignment(consignmentId, UUID.fromString(otherUserId))

    val response: GraphqlQueryData = runTestQuery("query_somedata", validUserToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "getConsignment" should "return an error if a user queries without a consignment id argument" in {
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_error_no_consignmentid")
    val response: GraphqlQueryData = runTestQuery("query_no_consignmentid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateExportLocation" should "update the export location correctly" in {
    createConsignment(new FixedUUIDSource().uuid, userId)
    val prefix = "json/updateexportlocation_"
    val expectedResponse = getDataFromFile[GraphqlMutationExportLocation](prefix)("data_all")
    val token = validBackendChecksToken("export")
    val response: GraphqlMutationExportLocation = runTestRequest[GraphqlMutationExportLocation](prefix)("mutation_all", token)
    response.data should equal(expectedResponse.data)
    getConsignmentField(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"), "ExportLocation") should equal("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e.tar.gz")
  }

  "updateTransferInitiated" should "update the transfer initiated date correctly" in {
    createConsignment(new FixedUUIDSource().uuid, userId)
    val prefix = "json/updatetransferinitiated_"
    val expectedResponse = getDataFromFile[GraphqlMutationTransferInitiated](prefix)("data_all")
    val response: GraphqlMutationTransferInitiated = runTestRequest[GraphqlMutationTransferInitiated](prefix)("mutation_all", validUserToken())
    response.data should equal(expectedResponse.data)
    val field = getConsignmentField(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"), _)
    Option(field("TransferInitiatedDatetime")).isDefined should equal(true)
    field("TransferInitiatedBy") should equal(userId.toString)
  }

  "consignments" should "allow a user with reporting access to return consignments in a paginated format" in {
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

    setUpConsignments(consignmentParams)

    val reportingAccessToken = validReportingToken("reporting")

    val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_all")
    val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_alldata", reportingAccessToken)

    response.data.get.consignments should equal(expectedResponse.data.get.consignments)
  }

  "consignments" should "allow a user with reporting access to return requested fields for consignments in a paginated format" in {
    val consignmentParams: List[ConsignmentParams] = List(
      ConsignmentParams(UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"), "consignment-ref1", List()),
      ConsignmentParams(UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"), "consignment-ref2", List()),
      ConsignmentParams(UUID.fromString("e6dadac0-0666-4653-b462-adca0b988095"), "consignment-ref3", List())
    )

    setUpConsignments(consignmentParams)
    val reportingAccessToken = validReportingToken("reporting")

    val expectedResponse: GraphqlConsignmentsQueryData = expectedConsignmentsQueryResponse("data_some")
    val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", reportingAccessToken)
    response.data.get.consignments.edges.size should equal(2)

    response.data should equal(expectedResponse.data)
  }

  "consignments" should "throw an error if user does not have reporting access" in {
    val exportAccessToken = invalidReportingToken()
    val response: GraphqlConsignmentsQueryData = runConsignmentsTestQuery("query_somedata", exportAccessToken)

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "startUpload" should "add the upload status and update the parent folder" in {
    val consignmentId = new FixedUUIDSource().uuid
    createConsignment(consignmentId, userId)
    runTestStartUploadMutation("mutation_alldata", validUserToken())

    val consignment = getConsignment(consignmentId)
    consignment.getString("ParentFolder") should equal("parent")
    getConsignmentStatus(consignmentId, "Upload").getString("Value") should equal("InProgress")
  }

  "startUpload" should "return an error if the upload is in progress" in {

    val consignmentId = new FixedUUIDSource().uuid
    createConsignment(consignmentId, userId)
    createConsignmentStatus(consignmentId, "Upload", "InProgress")
    val response = runTestStartUploadMutation("mutation_alldata", validUserToken())

    response.errors.size should equal(1)
    response.errors.head.message should equal("Existing consignment upload status is 'InProgress', so cannot start new upload")
  }

  "startUpload" should "return an error if the upload is complete" in {

    val consignmentId = new FixedUUIDSource().uuid
    createConsignment(consignmentId, userId)
    createConsignmentStatus(consignmentId, "Upload", "Complete")
    val response = runTestStartUploadMutation("mutation_alldata", validUserToken())

    response.errors.size should equal(1)
    response.errors.head.message should equal("Existing consignment upload status is 'Complete', so cannot start new upload")
  }

  private def checkConsignmentExists(consignmentId: UUID): Unit = {
    val result = getConsignment(consignmentId)
    result.getString("ConsignmentId") should equal(consignmentId.toString)
  }

  private def getConsignmentField(consignmentId: UUID, field: String): String = {
    val result = getConsignment(consignmentId)
    result.getString(field)
  }

  private def setUpConsignments(consignmentParams: List[ConsignmentParams]): Unit = {
    val seriesId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    addSeries(seriesId, fixedBodyId, "Mock series")

    consignmentParams.foreach(ps => {
      createConsignment(ps.consignmentId, userId, seriesId, consignmentRef = ps.consignmentRef, bodyId = fixedBodyId)
      createConsignmentStatus(ps.consignmentId, "Upload", "Completed")
      addParentFolderName(ps.consignmentId, "ALL CONSIGNMENT DATA PARENT FOLDER")
      ps.fileIds.foreach(fs => {
        setUpFileAndStandardMetadata(ps.consignmentId, fs)
      })
    })
  }

  private def setUpFileAndStandardMetadata(consignmentId: UUID, fileId: UUID): Unit = {
    createFile(fileId, consignmentId)
    generateMetadataPropertiesForFile(fileId)
    addAntivirusMetadata(fileId.toString)
    addFileMetadata(UUID.randomUUID().toString, fileId.toString, SHA256ServerSideChecksum)
    setUpStandardFFIDMatchesForFile(fileId)
  }

  private def generateMetadataPropertiesForFile(fileId: UUID): Unit = {
    staticMetadataProperties.foreach(smp => addFileMetadata(UUID.randomUUID().toString, fileId.toString, smp.name, smp.value))
    clientSideProperties.foreach { csp =>
      addFileMetadata(UUID.randomUUID().toString, fileId.toString, csp,
        csp match {
          case ClientSideFileLastModifiedDate => s"2021-02-08 16:00:00"
          case ClientSideFileSize => "1"
          case _ => s"$csp value"
        }
      )
    }
  }

  private def setUpStandardFFIDMatchesForFile(fileId: UUID): Unit = {
    val extensionMatch = "txt"
    val identificationBasisMatch = "TEST DATA identification"
    val puidMatch = "TEST DATA puid"

    val fileFfidMetadataId = addFFIDMetadata(fileId.toString)
    addFFIDMetadataMatches(fileFfidMetadataId.toString, extensionMatch, identificationBasisMatch, puidMatch)
  }
}

case class ConsignmentParams(consignmentId: UUID, consignmentRef: String, fileIds: List[UUID])
