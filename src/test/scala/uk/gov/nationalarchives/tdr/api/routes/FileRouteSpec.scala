package uk.gov.nationalarchives.tdr.api.routes

import java.sql.PreparedStatement
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
  private val getFilesJsonFilePrefix: String = "json/getfiles_"
  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationDataFilesMetadata(data: Option[AddFilesAndMetadata], errors: List[GraphqlError] = Nil)
  case class GraphqlQueryData(data: Option[GetFiles], errors: List[GraphqlError] = Nil)
  case class File(fileIds: Seq[UUID])
  case class GetFiles(getFiles: File)
  case class FileMatches(fileId: UUID, matchId: Long)
  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileMatches])

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](getFilesJsonFilePrefix)
  val runTestMutationFileMetadata: (String, OAuth2BearerToken) => GraphqlMutationDataFilesMetadata =
    runTestRequest[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedFilesAndMetadataMutationResponse: String => GraphqlMutationDataFilesMetadata =
    getDataFromFile[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](getFilesJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

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
