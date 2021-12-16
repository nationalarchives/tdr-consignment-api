package uk.gov.nationalarchives.tdr.api.routes

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{FFID, NonJudgmentFormat, PasswordProtected, Success, Zip}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestRequest}

class FFIDMetadataRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase {

  private val addFfidMetadataJsonFilePrefix: String = "json/addffidmetadata_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlMutationData(data: Option[AddFFIDMetadata], errors: List[GraphqlError] = Nil)
  case class AddFFIDMetadata(addFFIDMetadata: FFIDMetadata)

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData =
    runTestRequest[GraphqlMutationData](addFfidMetadataJsonFilePrefix)

  val expectedMutationResponse: String => GraphqlMutationData =
    getDataFromFile[GraphqlMutationData](addFfidMetadataJsonFilePrefix)

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  "addFFIDMetadata" should "return all requested fields from inserted file format object" in {
    seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_all")
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("file_format"))
    val metadata: FFIDMetadata = response.data.get.addFFIDMetadata
    val expectedMetadata = expectedResponse.data.get.addFFIDMetadata

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

    checkFFIDMetadataExists(response.data.get.addFFIDMetadata.fileId)
  }

  "addFFIDMetadata" should "set a single file status to 'Success' when a success match only is found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_alldata", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Success)
  }

  "addFFIDMetadata" should "set a single file status to 'Success' when a success match only is found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_judgment_format", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Success)
  }

  "addFFIDMetadata" should
      "set a single file status to 'Success' when a non judgment match only is found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_non_judgment_format", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Success)
  }

  "addFFIDMetadata" should
      "set a single file status to 'NonJudgmentRecord' when a non judgment match only is found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_non_judgment_format", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(NonJudgmentFormat)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Success' when there are multiple success matches only found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_multiple_success", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Success)
  }

  "addFFIDMetadata" should
      "set a single file status of 'PasswordProtected' when a password protected match only is found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(PasswordProtected)
  }

  "addFFIDMetadata" should
      "set a single file status of 'PasswordProtected' when a password protected match only is found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(PasswordProtected)
  }

  "addFFIDMetadata" should
      "set a single file status of 'PasswordProtected' when multiple password protected matches are found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_multiple_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(PasswordProtected)
  }

  "addFFIDMetadata" should
      "set a single file status of 'PasswordProtected' when multiple password protected matches are found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_multiple_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(PasswordProtected)
  }

  "addFFIDMetadata" should
      "set a single status of 'PasswordProtected' when password protected and success matches are found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_password_protected_success", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.contains(PasswordProtected) should be(true)
  }

  "addFFIDMetadata" should
      "set a single status of 'PasswordProtected' when password protected and success matches are found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_judgment_password_protected_success", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.contains(PasswordProtected) should be(true)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when a zip match only is found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_zip", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.contains(Zip) should be(true)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when a zip match only is found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_zip", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.contains(Zip) should be(true)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when multiple zip matches are found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_multiple_zip", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Zip)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when multiple zip matches are found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_multiple_zip", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Zip)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when zip and success matches are found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_zip_success", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Zip)
  }

  "addFFIDMetadata" should
      "set a single file status of 'Zip' when zip and success matches are found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_judgment_zip_success", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(1)
    result.head should equal(Zip)
  }

  "addFFIDMetadata" should
      "set multiple file statuses when zip and password protected matches are found for 'standard' consignment type" in {
    seedDatabaseWithDefaultEntries()

    runTestMutation("mutation_status_zip_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(2)
    result.contains(Zip) should be(true)
    result.contains(PasswordProtected) should be(true)
  }

  "addFFIDMetadata" should
      "set multiple file statuses when zip and password protected matches are found for 'judgment' consignment type" in {
    seedDatabaseWithDefaultEntries("judgment")

    runTestMutation("mutation_status_zip_password_protected", validBackendChecksToken("file_format"))

    val result = getFileStatusResult(defaultFileId, FFID)
    result.size should be(2)
    result.contains(Zip) should be(true)
    result.contains(PasswordProtected) should be(true)
  }

  "addFFIDMetadata" should "not allow updating of file format metadata with incorrect authorisation" in {
    seedDatabaseWithDefaultEntries()
    val response: GraphqlMutationData = runTestMutation("mutation_alldata", invalidBackendChecksToken())

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFFIDMetadataAdded()
  }

  "addFFIDMetadata" should "not allow updating of file format metadata with incorrect client role" in {
    seedDatabaseWithDefaultEntries()

    val response: GraphqlMutationData = runTestMutation("mutation_alldata", validBackendChecksToken("antivirus"))

    response.errors should have size 1
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
    checkNoFFIDMetadataAdded()
  }

  "addFFIDMetadata" should "throw an error if mandatory fields are missing" in {
    seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_mandatory_missing")
    val response: GraphqlMutationData = runTestMutation("mutation_mandatorymissing", validBackendChecksToken("file_format"))
    response.errors.map(e => e.message.trim) should equal (expectedResponse.errors.map(_.message.trim))
    checkNoFFIDMetadataAdded()
  }

  "addFFIDMetadata" should "throw an error if the file id does not exist" in {
    seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_fileid_not_exists")
    val response: GraphqlMutationData = runTestMutation("mutation_fileidnotexists", validBackendChecksToken("file_format"))
    response.errors.head.message should equal (expectedResponse.errors.head.message)
    checkNoFFIDMetadataAdded()
  }

  "addFFIDMetadata" should "throw an error if there are no ffid matches" in {
    seedDatabaseWithDefaultEntries()

    val expectedResponse: GraphqlMutationData = expectedMutationResponse("data_no_ffid_matches")
    val response: GraphqlMutationData = runTestMutation("mutation_no_ffid_matches", validBackendChecksToken("file_format"))
    response.errors.head.extensions should equal (expectedResponse.errors.head.extensions)
    response.errors.head.message should equal (expectedResponse.errors.head.message)
    checkNoFFIDMetadataAdded()
  }

  private def checkFFIDMetadataExists(fileId: UUID): Unit = {
    val sql = "select * from FFIDMetadata where FileId = ?;"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId.toString)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("FileId") should equal(fileId.toString)
  }

  private def resetDatabase(): Unit = {
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM FFIDMetadataMatches").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM FFIDMetadata").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM FileMetadata").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM FileProperty").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM File").executeUpdate()
    DbConnection.db.source.createConnection().prepareStatement("DELETE FROM Consignment").executeUpdate()
  }

  private def checkNoFFIDMetadataAdded(): Unit = {
    val sql = "SELECT * FROM FFIDMetadata;"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    rs.last()
    rs.getRow should equal(0)
  }

}
