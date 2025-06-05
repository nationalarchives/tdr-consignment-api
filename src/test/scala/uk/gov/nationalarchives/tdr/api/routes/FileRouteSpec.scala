package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, Types}
import java.util.UUID

class FileRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  case class FileNameAndPath(fileName: String, path: String)

  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationDataFilesMetadata(data: Option[AddFilesAndMetadata], errors: List[GraphqlError] = Nil)
  case class GraphqlQueryDataAllDescendants(data: Option[AllDescendants], errors: List[GraphqlError] = Nil)

  case class FileMatches(fileId: UUID, matchId: Long)

  case class File(fileId: UUID, fileType: Option[String], fileName: Option[String], parentId: Option[UUID])

  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileMatches])
  case class AllDescendants(allDescendants: List[File])

  val runTestMutationFileMetadata: (String, OAuth2BearerToken) => GraphqlMutationDataFilesMetadata =
    runTestRequest[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)
  val expectedFilesAndMetadataMutationResponse: String => GraphqlMutationDataFilesMetadata =
    getDataFromFile[GraphqlMutationDataFilesMetadata](addFilesAndMetadataJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

  "The api" should "add files and metadata entries for files and directories" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("c44f1b9b-1275-4bc3-831c-808c50a0222d")
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty(_))
    utils.createConsignment(consignmentId, userId)

    defaultMetadataProperties.foreach { defaultMetadataProperty =>
      utils.createFilePropertyValues(
        defaultMetadataProperty,
        defaultMetadataProperty match {
          case RightsCopyright  => defaultCopyright
          case LegalStatus      => defaultLegalStatus
          case HeldBy           => defaultHeldBy
          case Language         => defaultLanguage
          case FoiExemptionCode => defaultFoiExemptionCode
        },
        default = true,
        1,
        1
      )
    }
    val referenceMockServer = getReferencesMockServer(4)
    val res = runTestMutationFileMetadata("mutation_alldata_2", validUserToken())
    val distinctDirectoryCount = 3
    val fileCount = 5
    val expectedCount = ((FileUUID :: Filename :: FileType :: FileReference :: ParentReference :: defaultMetadataProperties).size * distinctDirectoryCount) +
      (defaultMetadataProperties.size * fileCount) +
      (clientSideProperties.size * fileCount) +
      (serverSideProperties.size * fileCount) +
      distinctDirectoryCount

    utils.countAllFileMetadata() should equal(expectedCount)

    res.data.get.addFilesAndMetadata
      .map(_.fileId)
      .foreach(fileId => {
        val nameAndPath = getFileNameAndOriginalPathMatch(fileId, utils)
        nameAndPath.isDefined should equal(true)
        nameAndPath.get.fileName should equal(nameAndPath.get.path.split("/").last)
      })
    referenceMockServer.stop()
  }

  "The api" should "return file ids matched with sequence ids for addFilesAndMetadata" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("1cd5e07a-34c8-4751-8e81-98edd17d1729")
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty(_))
    utils.createConsignment(consignmentId, userId)

    val referenceMockServer = getReferencesMockServer(4)

    val expectedResponse = expectedFilesAndMetadataMutationResponse("data_all")
    val response = runTestMutationFileMetadata("mutation_alldata_3", validUserToken())
    response.data.get.addFilesAndMetadata should equal(expectedResponse.data.get.addFilesAndMetadata)
    referenceMockServer.stop()
  }

  def getFileNameAndOriginalPathMatch(fileId: UUID, utils: TestUtils): Option[FileNameAndPath] = {
    val sql =
      """SELECT "FileName", "Value" FROM "FileMetadata" fm
        |JOIN "File" f on f."FileId" = fm."FileId" WHERE f."FileId" = ? AND "PropertyName" = 'ClientSideOriginalFilepath' """.stripMargin
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs = ps.executeQuery()
    if (rs.next()) {
      val fileName = rs.getString("FileName")
      val value = rs.getString("Value")
      Option(FileNameAndPath(fileName, value))
    } else {
      None
    }
  }

  private def getReferencesMockServer(additionalRefs: Int = 0): WireMockServer = {
    val wiremockServer = new WireMockServer(8008)
    WireMock.configureFor("localhost", 8008)
    wiremockServer.start()
    wiremockServer.stubFor(
      WireMock
        .get(WireMock.urlPathMatching("/test/.*"))
        .inScenario("fetch references")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("[\"REF1\",\"REF2\"]")
        )
        .willSetStateTo("fetch references 1")
    )
    for (current <- 1 to additionalRefs) {
      wiremockServer.stubFor(
        WireMock
          .get(WireMock.urlPathMatching("/test/.*"))
          .inScenario("fetch references")
          .whenScenarioStateIs(s"fetch references $current")
          .willReturn(
            aResponse()
              .withStatus(200)
              .withHeader("Content-Type", "application/json")
              .withBody(s"[\"REF${Math.random()}\",\"REF${Math.random()}\"]")
          )
          .willSetStateTo(s"fetch references ${current + 1}")
      )
    }
    wiremockServer
  }
}
