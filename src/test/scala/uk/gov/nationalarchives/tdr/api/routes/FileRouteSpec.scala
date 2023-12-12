package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._

import java.sql.{PreparedStatement, Types}
import java.util.UUID

class FileRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  case class FileNameAndPath(fileName: String, path: String)

  private val addFilesAndMetadataJsonFilePrefix: String = "json/addfileandmetadata_"
  private val allDescendantsJsonFilePrefix: String = "json/alldescendants_"

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

  val runTestQueryAllDescendants: (String, OAuth2BearerToken) => GraphqlQueryDataAllDescendants =
    runTestRequest[GraphqlQueryDataAllDescendants](allDescendantsJsonFilePrefix)
  val expectedAllDescendantsQueryResponse: String => GraphqlQueryDataAllDescendants =
    getDataFromFile[GraphqlQueryDataAllDescendants](allDescendantsJsonFilePrefix)

  val fixedUuidSource = new FixedUUIDSource()

  "The api" should "add files and metadata entries for files and directories" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("c44f1b9b-1275-4bc3-831c-808c50a0222d")
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty)
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
    val res = runTestMutationFileMetadata("mutation_alldata_2", validUserToken())
    val distinctDirectoryCount = 3
    val fileCount = 5
    val expectedCount = ((Filename :: FileType :: FileReference :: ParentReference :: defaultMetadataProperties).size * distinctDirectoryCount) +
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
  }

  "The api" should "return file ids matched with sequence ids for addFilesAndMetadata" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("1cd5e07a-34c8-4751-8e81-98edd17d1729")
    val utils = TestUtils(container.database)
    (clientSideProperties ++ serverSideProperties ++ defaultMetadataProperties).foreach(utils.addFileProperty)
    utils.createConsignment(consignmentId, userId)

    val expectedResponse = expectedFilesAndMetadataMutationResponse("data_all")
    val response = runTestMutationFileMetadata("mutation_alldata_3", validUserToken())
    expectedResponse.data.get.addFilesAndMetadata should equal(response.data.get.addFilesAndMetadata)
  }

  "allDescendants" should "return parents and all descendants for the given parent ids" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_multiple_parentids")
    val response = runTestQueryAllDescendants("query_multiple_parentids", validUserToken())
    response.data.get.allDescendants should equal(expectedResponse.data.get.allDescendants)
  }

  "allDescendants" should "return parent and all descendants of the given single parent id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_single_parentid")
    val response = runTestQueryAllDescendants("query_single_parentid", validUserToken())
    response.data.get.allDescendants should equal(expectedResponse.data.get.allDescendants)
  }

  "allDescendants" should "only return parent where no descendants" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_no_descendants")
    val response = runTestQueryAllDescendants("query_no_descendants", validUserToken())
    response.data.get.allDescendants should equal(expectedResponse.data.get.allDescendants)
  }

  "allDescendants" should "return an empty response if no parent ids passed" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_no_parentids")
    val response = runTestQueryAllDescendants("query_no_parentids", validUserToken())
    response.data.get.allDescendants should equal(expectedResponse.data.get.allDescendants)
  }

  "allDescendants" should "not allow a user to get descendants for a consignment that they did not create" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val otherUserId = "73abd1dc-294d-4068-b60d-c1cd4782d08d"
    createConsignmentStructure(utils, UUID.fromString(otherUserId))
    val consignmentId = UUID.fromString("f1dbc692-e56c-4d76-be94-d8d3d79bd38a")

    utils.createConsignment(consignmentId, UUID.fromString(otherUserId))

    val response = runTestQueryAllDescendants("query_no_descendants", validUserToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "allDescendants" should "return an error where no consignment id input argument provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_no_consignmentid")
    val response = runTestQueryAllDescendants("query_no_consignmentid", validUserToken())

    response should equal(expectedResponse)
  }

  "allDescendants" should "return an error where no parent id input argument provided" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    createConsignmentStructure(utils)

    val expectedResponse = expectedAllDescendantsQueryResponse("data_no_parentids_input")
    val response = runTestQueryAllDescendants("query_no_parentids_input", validUserToken())

    response should equal(expectedResponse)
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
}
