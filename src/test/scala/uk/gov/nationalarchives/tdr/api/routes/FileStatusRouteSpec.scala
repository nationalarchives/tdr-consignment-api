package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.FileStatus
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{GraphqlError, fixedSeriesId, getDataFromFile}
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, TestContainerUtils, TestRequest, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Timestamp, Types}
import java.time.Instant
import java.util.UUID

class FileStatusRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFileStatusPrefix: String = "json/addFileStatuses_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlAddFileStatusMutationData(data: Option[AddMultipleFileStatus], errors: List[GraphqlError] = Nil)

  case class AddMultipleFileStatus(addMultipleFileStatuses: List[FileStatus])

  val runAddFileStatusTestMutation: (String, OAuth2BearerToken) => GraphqlAddFileStatusMutationData =
    runTestRequest[GraphqlAddFileStatusMutationData](addFileStatusPrefix)

  val expectedAddFileStatusMutationResponse: String => GraphqlAddFileStatusMutationData =
    getDataFromFile[GraphqlAddFileStatusMutationData](addFileStatusPrefix)
  val fixedUuidSource = new FixedUUIDSource()

  "addFileStatus" should "add file status with status type and value" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val token = validUserToken(userId)
    val consignmentId = UUID.fromString("eb197bfb-43f7-40ca-9104-8f6cbda88506")
    utils.createConsignment(consignmentId, userId, fixedSeriesId)
    utils.createFile(defaultFileId, consignmentId, userId=userId)

    val expectedResponse: GraphqlAddFileStatusMutationData = expectedAddFileStatusMutationResponse("data_all")
    val response: GraphqlAddFileStatusMutationData = runAddFileStatusTestMutation("mutation_alldata", token)
    print(response)
    response.data.get.addMultipleFileStatuses.head should equal(expectedResponse.data.get.addMultipleFileStatuses.head)
    checkFileStatusExists(defaultFileId, utils, expectedResponse.data.get.addMultipleFileStatuses.head)
  }

  "addFileStatus" should "not allow a user to add a file status of a File that they did not upload" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val consignmentId = UUID.fromString("eb197bfb-43f7-40ca-9104-8f6cbda88506")
    utils.createConsignment(consignmentId, userId, fixedSeriesId)
    utils.createFile(defaultFileId, consignmentId, userId=userId)

    val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
    val token = validUserToken(wrongUserId)

    val expectedResponse = expectedAddFileStatusMutationResponse("data_not_owner")
    val response = runAddFileStatusTestMutation("mutation_not_owner", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "addFileStatus" should "return an error if a files that doesn't exist is queried" in withContainers { case _: PostgreSQLContainer =>
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val token = validUserToken(userId)

    val expectedResponse = expectedAddFileStatusMutationResponse("data_invalid_fileid")
    val response = runAddFileStatusTestMutation("mutation_invalid_fileid", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  private def checkFileStatusExists(fileId: UUID, utils: TestUtils, fileStatus: FileStatus): Unit = {
    val sql = """SELECT * FROM "FileStatus" WHERE "FileId" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
    rs.getString("StatusType") should equal(fileStatus.statusType)
    rs.getString("Value") should equal(fileStatus.statusValue)
    rs.getTimestamp("createddatetime").before(Timestamp.from(Instant.now())) shouldBe true
  }
}
