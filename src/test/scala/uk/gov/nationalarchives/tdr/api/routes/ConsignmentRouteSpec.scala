package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
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
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestDatabase, TestRequest}

//scalastyle:off number.of.methods
class ConsignmentRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase {
  private val addConsignmentJsonFilePrefix: String = "json/addconsignment_"
  private val getConsignmentJsonFilePrefix: String = "json/getconsignment_"
  private val consignmentsJsonFilePrefix: String = "json/consignments_"

  private val defaultSeriesId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  private val defaultSeriesName = "Mock series"
  private val defaultTransferringBodyId = UUID.fromString("830f0315-e683-440e-90d0-5f4aa60388c6")
  private val defaultTransferringBodyCode = "default-transferring-body-code"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[GetConsignment], errors: List[GraphqlError] = Nil)
  case class GraphqlConsignmentsQueryData(data: Option[ConsignmentConnections], errors: List[GraphqlError] = Nil)
  case class GraphqlMutationData(data: Option[AddConsignment], errors: List[GraphqlError] = Nil)
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
                         currentStatus: Option[CurrentStatus] = None
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

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val runConsignmentsTestQuery: (String, OAuth2BearerToken) =>
    GraphqlConsignmentsQueryData = runTestRequest[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addConsignmentJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getConsignmentJsonFilePrefix)
  val expectedConsignmentsQueryResponse: String =>
    GraphqlConsignmentsQueryData = getDataFromFile[GraphqlConsignmentsQueryData](consignmentsJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addConsignmentJsonFilePrefix)

  override def beforeEach(): Unit = {
    super.beforeEach()

    addTransferringBody(defaultTransferringBodyId, "Default transferring body name", defaultTransferringBodyCode)
  }

  "addConsignment" should "create a consignment if the correct information is provided" in {
    addSeries(defaultSeriesId, bodyId = defaultTransferringBodyId, "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = defaultTransferringBodyCode))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    checkConsignmentExists(response.data.get.addConsignment.consignmentid.get)
  }

  "addConsignment" should "throw an error if the series id field isn't provided" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_seriesid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingseriesid", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "addConsignment" should "link a new consignment to the creating user" in {
    addSeries(defaultSeriesId, bodyId = defaultTransferringBodyId, "Mock series")

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = defaultTransferringBodyCode))
    response.data.get.addConsignment should equal(expectedResponse.data.get.addConsignment)

    response.data.get.addConsignment.userid should contain(userId)
  }

  "addConsignment" should "not allow a user to link a consignment to a series from another transferring body" in {
    addSeries(defaultSeriesId, bodyId = defaultTransferringBodyId, "Mock series")

    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken(body = "some-other-transferring-body"))

    response.errors.size should be(1)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "getConsignment" should "return all requested fields" in {
    val consignmentId = UUID.fromString("b130e097-2edc-4e67-a7e9-5364a09ae9cb")

    val fileOneId = UUID.fromString("e7ba59c9-5b8b-4029-9f27-2d03957463ad")
    val fileTwoId = UUID.fromString("42910a85-85c3-40c3-888f-32f697bfadb6")

    val consignmentParams = List(
      (consignmentId, "TEST-TDR-2021-MTB", List(fileOneId, fileTwoId))
    )

    setUpConsignments(consignmentParams)

    val noAVAndChecksumFileId = UUID.fromString("9757f402-ee1a-43a2-ae2a-81a9ea9729b9")
    createFile(noAVAndChecksumFileId, consignmentId)
    generateMetadataPropertiesForFile(noAVAndChecksumFileId)
    setUpStandardFFIDMatchesForFile(noAVAndChecksumFileId)
    
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
    val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(body = defaultTransferringBodyCode))

    response should equal(expectedResponse)
  }

  "getConsignment" should "return the file metadata" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    val consignmentParams = List(
      (consignmentId, "consignment-ref", List(fileId))
    )

    setUpConsignments(consignmentParams)

    val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())
    val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_file_metadata")

    response should equal(expectedResponse)
  }

  "getConsignment" should "return empty ffid metadata if the ffid metadata is missing" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    createConsignment(consignmentId, userId, UUID.randomUUID())
    createFile(fileId, consignmentId)
    generateMetadataPropertiesForFile(fileId)

    val response: GraphqlQueryData = runTestQuery("query_filemetadata", validUserToken())

    response.data.get.getConsignment.get.files.get.head.ffidMetadata.isEmpty should be(true)
  }

  "getConsignment" should "return multiple droid matches" in {
    val consignmentId = UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c")
    val fileId = UUID.fromString("3ce8ef99-a999-4bae-8425-325a67f2d3da")

    createConsignment(consignmentId, userId, UUID.randomUUID())
    createFile(fileId, consignmentId)
    generateMetadataPropertiesForFile(fileId)

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

    response.errors.size should be(1)
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
    val consignmentParams: List[(UUID, String, List[UUID])] = List(
      (UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"),
        "consignment-ref1",
        List(UUID.fromString("9b003759-a9a2-4bf9-8e34-14079bdaed58"))),
      (UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"),
        "consignment-ref2",
        List(UUID.fromString("62c53beb-84d6-4676-80ea-b43f5329de72"))),
      (UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a"),
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
    val consignmentParams = List(
      (UUID.fromString("c31b3d3e-1931-421b-a829-e2ef4cd8930c"), "consignment-ref1", List()),
      (UUID.fromString("5c761efa-ae1a-4ec8-bb08-dc609fce51f8"), "consignment-ref2", List()),
      (UUID.fromString("e6dadac0-0666-4653-b462-adca0b988095"), "consignment-ref3", List())
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

    response.errors.size should be(1)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  private def checkConsignmentExists(consignmentId: UUID): Unit = {
    val result = getConsignment(consignmentId)
    result.getString("ConsignmentId") should equal(consignmentId.toString)
  }

  private def getConsignmentField(consignmentId: UUID, field: String): String = {
    val result = getConsignment(consignmentId)
    result.getString(field)
  }

  private def setUpConsignments(consignmentParams: List[(UUID, String, List[UUID])]): Unit = {
    addSeries(defaultSeriesId, defaultTransferringBodyId, defaultSeriesName)

    consignmentParams.foreach(ps => {
      createConsignment(ps._1, userId, defaultSeriesId, consignmentRef = ps._2)
      createConsignmentStatus(ps._1, "Upload", "Completed")
      addParentFolderName(ps._1, "ALL CONSIGNMENT DATA PARENT FOLDER")
      ps._3.foreach(fs => {
        setUpFileAndStandardMetadata(ps._1, fs)
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
