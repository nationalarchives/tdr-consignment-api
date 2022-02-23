package uk.gov.nationalarchives.tdr.api.routes.transfer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{clientSideProperties, staticMetadataProperties}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest}

import java.sql.PreparedStatement
import java.util.UUID

class FileRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationDataFilesMetadata(data: Option[AddFilesAndMetadata], errors: List[GraphqlError] = Nil)

  case class File(fileIds: Seq[UUID])

  case class FileMatches(fileId: UUID, matchId: Long)

  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileMatches])

  val runTestMutationFileMetadata: (String, OAuth2BearerToken, Config) => GraphqlMutationDataFilesMetadata =
    runTestRequest[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedFilesAndMetadataMutationResponse: String => GraphqlMutationDataFilesMetadata =
    getDataFromFile[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

  "The api" should "return file ids matched with sequence ids for addFilesAndMetadata" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("f1a9269d-157b-402c-98d8-1633393634c5")
      val utils = databaseUtils(container)
      utils.createConsignment(consignmentId, userId)
      clientSideProperties.foreach(csp => utils.addFileProperty(csp))
      staticMetadataProperties.foreach(smp => utils.addFileProperty(smp.name))
      val expectedResponse = expectedFilesAndMetadataMutationResponse("data_all")
      val response = runTestMutationFileMetadata("mutation_alldata", validUserToken(), config(container))

      expectedResponse.data.get.addFilesAndMetadata should equal(response.data.get.addFilesAndMetadata)
  }

  def checkStaticMetadataExists(fileId: UUID, config: Config): List[Assertion] = {
    staticMetadataProperties.map(property => {
      val sql = "SELECT * FROM FileMetadata WHERE FileId = ? AND PropertyName = ?"
      val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
      ps.setString(1, fileId.toString)
      ps.setString(2, property.name)
      val result = ps.executeQuery()
      result.next()
      result.getString("Value") should equal(property.value)
    })
  }
}
