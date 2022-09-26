package uk.gov.nationalarchives.tdr.api.routes

import cats.implicits.catsSyntaxOptionId
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.sql.ResultSet
import java.util.UUID

class AntivirusMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addAVMetadataJsonFilePrefix: String = "json/addavmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddAntivirusMetadata], errors: List[GraphqlError] = Nil)

  case class AntivirusMetadata(
                                fileId: UUID,
                                software: Option[String] = None,
                                softwareVersion: Option[String] = None,
                                databaseVersion: Option[String] = None,
                                result: Option[String] = None,
                                datetime: Long
                              )

  case class AddAntivirusMetadata(addAntivirusMetadata: AntivirusMetadata) extends TestRequest

  val runTestMutation: (String, String) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addAVMetadataJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addAVMetadataJsonFilePrefix)

  "addAntivirusMetadata" should "return all requested fields from inserted antivirus metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val consignmentId = UUID.randomUUID()
      utils.createConsignment(consignmentId, userId)
      utils.createFile(UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313"), consignmentId)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
      val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))
      response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

      checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId, utils)
  }

  "addAntivirusMetadata" should "return the expected data from inserted antivirus metadata object" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = utils.createConsignment(UUID.randomUUID())
      utils.createFile(UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313"), consignmentId)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_some")
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", validBackendChecksToken("antivirus"))
      response.data.get.addAntivirusMetadata should equal(expectedResponse.data.get.addAntivirusMetadata)

      checkAntivirusMetadataExists(response.data.get.addAntivirusMetadata.fileId, utils)
  }

  "addAntivirusMetadata" should "not allow updating of antivirus metadata with incorrect authorisation" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val response: GraphqlMutationData = runTestMutation("mutation_somedata", invalidBackendChecksToken())

      response.errors should have size 1
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
      checkNoAntivirusMetadataAdded(utils)
  }

  "addAntivirusMetadata" should "throw an error if the field file id is not provided" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_missing")
      val response: GraphqlMutationData = runTestMutation("mutation_missingfileid", validBackendChecksToken("antivirus"))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      checkNoAntivirusMetadataAdded(utils)
  }

  "addAntivirusMetadata" should "set the file status to virus found when there is a virus found" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val consignmentId = utils.createConsignment(UUID.randomUUID())
      utils.createFile(UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313"), consignmentId)

      runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

      val result = utils.getFileStatusResult(defaultFileId, Antivirus)
      result.size should be(1)
      result.head should equal(VirusDetected)
  }

  "addAntivirusMetadata" should "set the file status to success when there is no virus found" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val consignmentId = utils.createConsignment(UUID.randomUUID())
      utils.createFile(UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313"), consignmentId)
      runTestMutation("mutation_noresult", validBackendChecksToken("antivirus"))

      val result = utils.getFileStatusResult(defaultFileId, Antivirus)
      result.size should be(1)
      result.head should equal(Success)
  }

  private def checkAntivirusMetadataExists(fileId: UUID, utils: TestUtils): Unit = {
    val rs: ResultSet = utils.getAntivirusMetadata(fileId.some)
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNoAntivirusMetadataAdded(utils: TestUtils): Unit = {
    val rs: ResultSet = utils.getAntivirusMetadata()
    rs.next()
    rs.getRow should equal(0)
  }
}
