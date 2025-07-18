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

  case class FileMatches(fileId: UUID, matchId: Long)

  case class File(fileId: UUID, fileType: Option[String], fileName: Option[String], parentId: Option[UUID])

  case class AddFilesAndMetadata(addFilesAndMetadata: List[FileMatches])

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

        val actualUserId = getUserIdForFile(fileId, utils).get
        actualUserId should equal(userId)
      })
    referenceMockServer.stop()
  }

  "'addFilesAndMetadata' mutation" should "override user id where present in input for files" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("c44f1b9b-1275-4bc3-831c-808c50a0222d")
    val overrideUserId = userId
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty(_))
    utils.createConsignment(consignmentId, overrideUserId)

    val referenceMockServer = getReferencesMockServer(4)
    val res = runTestMutationFileMetadata("mutation_override_user_id", validTransferServiceToken("data-load"))
    res.data.get.addFilesAndMetadata
      .map(_.fileId)
      .foreach(fileId => {
        val userId = getUserIdForFile(fileId, utils).get
        userId should equal(overrideUserId)
      })
    referenceMockServer.stop()
  }

  "'addFilesAndMetadata' mutation" should "throw an error when the override user id does not match the consignment user id" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("c44f1b9b-1275-4bc3-831c-808c50a0222d")
      val differentUserId = UUID.randomUUID()
      val utils = TestUtils(container.database)
      (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty(_))
      utils.createConsignment(consignmentId, differentUserId)

      val referenceMockServer = getReferencesMockServer(4)
      val res = runTestMutationFileMetadata("mutation_incorrect_override_user_id", validTransferServiceToken("data-load"))
      res.errors.head.message should equal("User '9d7fbffc-4012-40a8-89bf-314eabab78a8' does not have access to consignment 'c44f1b9b-1275-4bc3-831c-808c50a0222d'")
      res.data.isDefined shouldBe false
      referenceMockServer.stop()
  }

  "'addFilesAndMetadata' mutation" should "throw an error if user does  not have permission to override user id" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("c44f1b9b-1275-4bc3-831c-808c50a0222d")
    val overrideUserId = userId
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty(_))
    utils.createConsignment(consignmentId, overrideUserId)

    val referenceMockServer = getReferencesMockServer(4)
    val res = runTestMutationFileMetadata("mutation_override_user_id", validTransferServiceToken("some-random-role"))
    res.errors.head.message should equal("User '5be4be46-cbd3-4073-8500-3be04522145d' does not have access to consignment 'c44f1b9b-1275-4bc3-831c-808c50a0222d'")
    res.data.isDefined shouldBe false
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

  def getUserIdForFile(fileId: UUID, utils: TestUtils): Option[UUID] = {
    val sql =
      """SELECT "UserId" FROM "File" where "FileId" = ?""".stripMargin
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs = ps.executeQuery()
    if (rs.next()) {
      val userId = rs.getString("UserId")
      Option(UUID.fromString(userId))
    } else {
      None
    }
  }

  private def createConsignmentStructure(utils: TestUtils, userId: UUID = userId): Unit = {
    val consignmentId = UUID.fromString("1cd5e07a-34c8-4751-8e81-98edd17d1729")
    val rootFolderId = "90f56dfc-c6a2-4b59-add7-18e256ae864a"
    val folderId: String = "7b19b272-d4d1-4d77-bf25-511dc6489d12"
    val folderId1: String = "d94b80ea-54c0-4357-9ff3-e097efcebb9b"
    val subFolderId: String = "42910a85-85c3-40c3-888f-32f697bfadb6"
    val fileOneId = "e7ba59c9-5b8b-4029-9f27-2d03957463ad"
    val fileTwoId = "8d9e6de0-e92f-4fef-b8e9-201322937c9b"
    val fileThreeId = "9757f402-ee1a-43a2-ae2a-81a9ea9729b9"
    val fileFourId = "a5dc822f-8d6a-4ef6-9f7a-5077fa9b318c"
    val fileFiveId = "9cda9ee6-00f1-4c50-a708-1eac0d48623e"

    utils.createConsignment(consignmentId, userId, fixedSeriesId, "TEST-TDR-2021-MTB")
    utils.createFile(UUID.fromString(folderId), consignmentId, "Folder", "folderName", Some(UUID.fromString(rootFolderId)))
    utils.createFile(UUID.fromString(folderId1), consignmentId, "Folder", "folderOneName", Some(UUID.fromString(rootFolderId)))
    utils.createFile(UUID.fromString(subFolderId), consignmentId, "Folder", "subFolderName", Some(UUID.fromString(folderId)))
    utils.createFile(UUID.fromString(fileOneId), consignmentId, fileName = "fileOneName", parentId = Some(UUID.fromString(folderId)))
    utils.createFile(UUID.fromString(fileTwoId), consignmentId, fileName = "fileTwoName", parentId = Some(UUID.fromString(folderId)))
    utils.createFile(UUID.fromString(fileThreeId), consignmentId, fileName = "fileThreeName", parentId = Some(UUID.fromString(subFolderId)))
    utils.createFile(UUID.fromString(fileFourId), consignmentId, fileName = "fileFourName", parentId = Some(UUID.fromString(folderId1)))
    utils.createFile(UUID.fromString(fileFiveId), consignmentId, fileName = "fileFiveName", parentId = Some(UUID.fromString(folderId1)))
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
