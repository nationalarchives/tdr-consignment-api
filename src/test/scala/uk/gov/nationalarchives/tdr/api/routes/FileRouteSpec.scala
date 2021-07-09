package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.staticMetadataProperties
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestDatabase, TestRequest}

class FileRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase  {
  private val addFileJsonFilePrefix: String = "json/addfile_"
  private val getFilesJsonFilePrefix: String = "json/getfiles_"
  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationDataFilesMetadata(data: Option[AddFilesAndMetadata], errors: List[GraphqlError] = Nil)
  case class GraphqlMutationData(data: Option[AddFiles], errors: List[GraphqlError] = Nil)
  case class GraphqlQueryData(data: Option[GetFiles], errors: List[GraphqlError] = Nil)
  case class File(fileIds: Seq[UUID])
  case class AddFiles(addFiles: File)
  case class GetFiles(getFiles: File)
  case class FileSequence(fileId: UUID, sequenceNumber: Long)
  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileSequence])

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getFilesJsonFilePrefix)
  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](addFileJsonFilePrefix)
  val runTestMutationFileMetadata: (String, OAuth2BearerToken) => GraphqlMutationDataFilesMetadata =
    runTestRequest[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](addFileJsonFilePrefix)
  val expectedFilesAndMetadataMutationResponse: String => GraphqlMutationDataFilesMetadata =
    getDataFromFile[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getFilesJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

  "The api" should "add one file if the correct information is provided" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_one_file")
    val response: GraphqlMutationData = runTestMutation("mutation_one_file", validUserToken())

    response.data.isDefined should equal(true)
    response.data.get.addFiles should equal(expectedResponse.data.get.addFiles)
    response.data.get.addFiles.fileIds.foreach(checkFileExists)
  }

  "The api" should "add the static metadata if the correct information is provided" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)

    val response: GraphqlMutationData = runTestMutation("mutation_one_file", validUserToken())

    response.data.get.addFiles.fileIds.foreach(checkStaticMetadataExists)
  }

  "The api" should "add three files if the correct information is provided" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())

    response.data.isDefined should equal(true)
    response.data.get.addFiles should equal(expectedResponse.data.get.addFiles)

    response.data.get.addFiles.fileIds.foreach(checkFileExists)
  }

  "The api" should "throw an error if the consignment id field is not provided" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_consignmentid_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingconsignmentid", validUserToken())
    response.errors.head.message should equal (expectedResponse.errors.head.message)
  }

  "The api" should "throw an error if the number of files field is not provided" in {
    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_numberoffiles_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_missingnumberoffiles", validUserToken())
    response.errors.head.message should equal (expectedResponse.errors.head.message)
  }

  "The api" should "throw an error if the user does not own the consignment" in {
    val userId = UUID.fromString("5ab14990-ed63-4615-8336-56fbb9960300")
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    createConsignment(consignmentId, userId)

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_not_owner")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validUserToken())
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "The api" should "throw an error if the consignment already has had files uploaded" in {
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    createConsignment(consignmentId, userId)
    //Seed DB with initial file for consignment
    runTestMutation("mutation_one_file", validUserToken())

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_error_previous_upload")
    val response: GraphqlMutationData = runTestMutation("mutation_one_file", validUserToken())
    response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
    response.errors.head.message should equal (expectedResponse.errors.head.message)
    checkNumberOfFiles(1)
  }

  "The api" should "return all available files" in {
    val consignmentId = UUID.fromString("50df01e6-2e5e-4269-97e7-531a755b417d")
    val fileIdOne = UUID.fromString("7b19b272-d4d1-4d77-bf25-511dc6489d12")
    val fileIdTwo = UUID.fromString("0f70f657-8b19-4ab6-9813-33a8223fec84")
    createFile(fileIdOne, consignmentId)
    createFile(fileIdTwo, consignmentId)
    addAntivirusMetadata(fileIdOne.toString, "")
    addAntivirusMetadata(fileIdTwo.toString, "")
    val expectedResponse = expectedQueryResponse("data_all")
    val response = runTestQuery("mutation_alldata", validBackendChecksToken("export"))

    expectedResponse.data.get.getFiles should equal(response.data.get.getFiles)
  }

  "The api" should "not return files with a virus" in {
    val consignmentId = UUID.fromString("fc13c325-71f8-4cf3-954d-38e212df3ff3")
    val fileIdOne = UUID.fromString("3976840e-adee-4cfa-8cee-6d790934e152")
    val fileIdTwo = UUID.fromString("d4aced21-3c3f-4007-bbb8-9e94967ff89e")
    createFile(fileIdOne, consignmentId)
    createFile(fileIdTwo, consignmentId)
    addAntivirusMetadata(fileIdOne.toString, "")
    addAntivirusMetadata(fileIdTwo.toString)
    val expectedResponse = expectedQueryResponse("data_onefile")
    val response = runTestQuery("mutation_onevirusfailed", validBackendChecksToken("export"))

    expectedResponse.data.get.getFiles should equal(response.data.get.getFiles)
  }

  "The api" should "return file ids matched with sequence ids for addFilesAndMetadata" in {
    val consignmentId = UUID.fromString("f1a9269d-157b-402c-98d8-1633393634c5")
    createConsignment(consignmentId, userId)

    val expectedResponse = expectedFilesAndMetadataMutationResponse("data_all")
    val response = runTestMutationFileMetadata("mutation_alldata", validUserToken())

    expectedResponse.data.get.addFilesAndMetadata should equal(response.data.get.addFilesAndMetadata)
  }

  private def checkFileExists(fileId: UUID) = {
    val sql = s"select * from File where FileId = ?"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId.toString)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNumberOfFiles(expectedNumberOfFiles: Int) = {
    val sql = s"select * from File"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs.last()
    rs.getRow should equal(expectedNumberOfFiles)
  }

  def checkStaticMetadataExists(fileId: UUID): List[Assertion] = {
    staticMetadataProperties.map(property => {
      val sql = "SELECT * FROM FileMetadata WHERE FileId = ? AND PropertyName = ?"
      val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
      ps.setString(1, fileId.toString)
      ps.setString(2, property.name)
      val result = ps.executeQuery()
      result.next()
      result.getString("Value") should equal(property.value)
    })
  }
}
