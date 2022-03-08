package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{clientSideProperties, staticMetadataProperties}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, Types}
import java.util.UUID

class FileRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationDataFilesMetadata(data: Option[AddFilesAndMetadata], errors: List[GraphqlError] = Nil)

  case class File(fileIds: Seq[UUID])

  case class FileMatches(fileId: UUID, matchId: Long)

  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileMatches])

  val runTestMutationFileMetadata: (String, OAuth2BearerToken) => GraphqlMutationDataFilesMetadata =
    runTestRequest[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedFilesAndMetadataMutationResponse: String => GraphqlMutationDataFilesMetadata =
    getDataFromFile[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

  "The api" should "return file ids matched with sequence ids for addFilesAndMetadata" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("f1a9269d-157b-402c-98d8-1633393634c5")
      val utils = TestUtils(container.database)
      (clientSideProperties ++ staticMetadataProperties.map(_.name)).foreach(utils.addFileProperty)
      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedFilesAndMetadataMutationResponse("data_all")
      val response = runTestMutationFileMetadata("mutation_alldata", validUserToken())

      expectedResponse.data.get.addFilesAndMetadata should equal(response.data.get.addFilesAndMetadata)
  }

  "The api" should "add files and metadata entries for files and directories" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("f1a9269d-157b-402c-98d8-1633393634c5")
      val utils = TestUtils(container.database)
      (clientSideProperties ++ staticMetadataProperties.map(_.name)).foreach(utils.addFileProperty)
      utils.createConsignment(consignmentId, userId)

      runTestMutationFileMetadata("mutation_alldata", validUserToken())
      val distinctDirectoryCount = 3
      val fileCount = 2
      val expectedCount = (staticMetadataProperties.size * distinctDirectoryCount) +
        (staticMetadataProperties.size * fileCount) +
        (clientSideProperties.size * fileCount) +
        distinctDirectoryCount

      utils.countAllFileMetadata() should equal(expectedCount)
 }

  def checkStaticMetadataExists(fileId: UUID, utils: TestUtils): List[Assertion] = {
    staticMetadataProperties.map(property => {
      val sql = """SELECT * FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?"""
      val ps: PreparedStatement = utils.connection.prepareStatement(sql)
      ps.setObject(1, fileId, Types.OTHER)
      ps.setString(2, property.name)
      val result = ps.executeQuery()
      result.next()
      result.getString("Value") should equal(property.value)
    })
  }
}
