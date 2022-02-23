package uk.gov.nationalarchives.tdr.api.routes.metadata

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest}

import java.util.UUID

class ClientFileMetadataRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  private val getClientFileMetadataJsonFilePrefix: String = "json/getclientfilemetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  case class GraphqlQueryData(data: Option[GetClientFileMetadata], errors: List[GraphqlError] = Nil)

  case class ClientFileMetadata(
                                 fileId: Option[UUID],
                                 originalPath: Option[String] = None,
                                 checksum: Option[String] = None,
                                 checksumType: Option[String] = None,
                                 lastModified: Option[Long] = None,
                                 fileSize: Option[Long] = None
                               )

  case class GetClientFileMetadata(getClientFileMetadata: ClientFileMetadata) extends TestRequest

  val runTestQuery: (String, OAuth2BearerToken, Config) => GraphqlQueryData =
    runTestRequest[GraphqlQueryData](getClientFileMetadataJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData =
    getDataFromFile[GraphqlQueryData](getClientFileMetadataJsonFilePrefix)

  "getClientFileMetadata" should "return the requested fields" in withContainers {
    case container: PostgreSQLContainer =>

      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_all")
      val response: GraphqlQueryData = runTestQuery("query_alldata", validBackendChecksToken("client_file_metadata"), config(container))
      val responseData: ClientFileMetadata = response.data.get.getClientFileMetadata
      val expectedData = expectedResponse.data.get.getClientFileMetadata
      responseData.fileId should equal(expectedData.fileId)
      responseData.originalPath should equal(expectedData.originalPath)
      responseData.checksum should equal(expectedData.checksum)
      responseData.checksumType should equal(expectedData.checksumType)
      responseData.fileSize should equal(expectedData.fileSize)
  }

  "getClientFileMetadata" should "throw an error if the file id does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_fileid_not_exists")
      val response: GraphqlQueryData = runTestQuery("query_fileidnotexists", validBackendChecksToken("client_file_metadata"), config(container))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }

  "getClientFileMetadata" should "throw an error if the user does not have the file format role" in withContainers {
    case container: PostgreSQLContainer =>
      databaseUtils(container).seedDatabaseWithDefaultEntries()
      val expectedResponse: GraphqlQueryData = expectedQueryResponse("data_no_file_format_role")
      val response: GraphqlQueryData = runTestQuery("query_alldata", validUserToken(), config(container))

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal(expectedResponse.errors.head.extensions.get.code)
  }
}
