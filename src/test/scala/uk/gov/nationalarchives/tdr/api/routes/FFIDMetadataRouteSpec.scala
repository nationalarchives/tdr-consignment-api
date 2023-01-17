package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

class FFIDMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val addFfidMetadataJsonFilePrefix: String = "json/addffidmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddFFIDMetadata], errors: List[GraphqlError] = Nil)

  case class AddFFIDMetadata(addBulkFFIDMetadata: List[FFIDMetadata])

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addFfidMetadataJsonFilePrefix)

  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addFfidMetadataJsonFilePrefix)

  "addFFIDMetadata" should "return all requested fields from inserted file format object" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("file_format"))
    val metadata: FFIDMetadata = response.data.get.addBulkFFIDMetadata.head
    val expectedMetadata = expectedResponse.data.get.addBulkFFIDMetadata.head

    metadata.fileId should equal(expectedMetadata.fileId)
    metadata.software should equal(expectedMetadata.software)
    metadata.softwareVersion should equal(expectedMetadata.softwareVersion)
    metadata.binarySignatureFileVersion should equal(expectedMetadata.binarySignatureFileVersion)
    metadata.containerSignatureFileVersion should equal(expectedMetadata.containerSignatureFileVersion)
    metadata.method should equal(expectedMetadata.method)

    metadata.matches.size should equal(1)
    val matches = metadata.matches.head
    val expectedMatches = expectedMetadata.matches.head
    matches.extension should equal(expectedMatches.extension)
    matches.identificationBasis should equal(expectedMatches.identificationBasis)
    matches.puid should equal(expectedMatches.puid)

    checkFFIDMetadataExists(response.data.get.addBulkFFIDMetadata.head.fileId, utils)
  }

  "addFFIDMetadata" should "not allow updating of file format metadata with incorrect authorisation" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", invalidBackendChecksToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFFIDMetadataAdded(utils)
  }

  "addFFIDMetadata" should "not allow updating of file format metadata with incorrect client role" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()

    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFFIDMetadataAdded(utils)
  }

  "addFFIDMetadata" should "throw an error if mandatory fields are missing" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_mandatory_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_mandatorymissing", validBackendChecksToken("file_format"))
    response.errors.map(e => e.message.trim) should equal(expectedResponse.errors.map(_.message.trim))
    checkNoFFIDMetadataAdded(utils)
  }

  "addFFIDMetadata" should "throw an error if the file id does not exist" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_not_exists")
    val response: GraphqlMutationData = runTestMutation("mutation_fileidnotexists", validBackendChecksToken("file_format"))
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFFIDMetadataAdded(utils)
  }

  "addFFIDMetadata" should "throw an error if there are no ffid matches" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    utils.seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_ffid_matches")
    val response: GraphqlMutationData = runTestMutation("mutation_no_ffid_matches", validBackendChecksToken("file_format"))
    response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
    response.errors.head.message should equal(expectedResponse.errors.head.message)
    checkNoFFIDMetadataAdded(utils)
  }

  private def checkFFIDMetadataExists(fileId: UUID, utils: TestUtils): Unit = {
    val sql = """select * from "FFIDMetadata" where "FileId" = ?;"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def checkNoFFIDMetadataAdded(utils: TestUtils): Unit = {
    val sql = """SELECT * FROM "FFIDMetadata";"""
    val ps: PreparedStatement = utils.connection.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getRow should equal(0)
  }

}
