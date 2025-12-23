package uk.gov.nationalarchives.tdr.api.routes

import java.time.Instant
import java.util.UUID
import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.FileCheckFailure
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.SHA256ClientSideChecksum
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._

import java.sql.Timestamp

class FileCheckFailuresRouteSpec extends TestContainerUtils with Matchers with TestRequest {

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  private val fileCheckFailuresJsonFilePrefix: String = "json/filecheckfailures_"

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class GraphqlQueryData(data: Option[FileCheckFailures], errors: List[GraphqlError] = Nil)

  case class FileCheckFailures(getFileCheckFailures: List[FileCheckFailure])

  val runTestQuery: (String, OAuth2BearerToken) => GraphqlQueryData = runTestRequest[GraphqlQueryData](fileCheckFailuresJsonFilePrefix)
  val expectedQueryResponse: String => GraphqlQueryData = getDataFromFile[GraphqlQueryData](fileCheckFailuresJsonFilePrefix)

  "The api" should "return an empty list when no file check failures exist" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e96c7ed-6b56-4c1f-b02c-13c1d4f85c36")
    val fileId = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8d029dd4284b")

    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileId, "Antivirus", "Success")
    
    val response: GraphqlQueryData = runTestQuery("query_no_filters", validReportingToken("reporting"))
    
    response.data.get.getFileCheckFailures shouldBe List.empty
    response.errors shouldBe empty
  }

  "The api" should "return file check failures for files with failed checks" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e96c7ed-6b56-4c1f-b02c-13c1d4f85c36")
    val fileOneId = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8d029dd4284b")
    val fileTwoId = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479")

    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)

    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Antivirus", "Failure")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "FFID", "Failure")

    utils.addAntivirusMetadata(fileOneId.toString, "virus")
    utils.addAntivirusMetadata(fileTwoId.toString, "")
    utils.addFFIDMetadata(fileOneId.toString)
    utils.addFFIDMetadata(fileTwoId.toString)

    val response: GraphqlQueryData = runTestQuery("query_no_filters", validReportingToken("reporting"))
    
    response.data.get.getFileCheckFailures.size shouldBe 2
    response.data.get.getFileCheckFailures.map(_.fileId).toSet shouldBe Set(fileOneId, fileTwoId)
  }

  "The api" should "filter file check failures by consignment id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentOneId = UUID.fromString("6e96c7ed-6b56-4c1f-b02c-13c1d4f85c36")
    val consignmentTwoId = UUID.fromString("8f47ac10b-58cc-4372-a567-0e02b2c3d47")
    val fileOneId = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8d029dd4284b")
    val fileTwoId = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479")

    utils.createConsignment(consignmentOneId, userId)
    utils.createConsignment(consignmentTwoId, userId)
    utils.createFile(fileOneId, consignmentOneId)
    utils.createFile(fileTwoId, consignmentTwoId)

    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Antivirus", "Failure")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Antivirus", "Failure")

    utils.addAntivirusMetadata(fileOneId.toString, "virus")
    utils.addAntivirusMetadata(fileTwoId.toString, "virus")
    utils.addFFIDMetadata(fileOneId.toString)
    utils.addFFIDMetadata(fileTwoId.toString)

    val response: GraphqlQueryData = runTestQuery("query_filtered_by_consignment", validReportingToken("reporting"))
    response.data.get.getFileCheckFailures.size shouldBe 1
    response.data.get.getFileCheckFailures.map(c => (c.fileId, c.consignmentId)).head should be (fileOneId, consignmentOneId)
    
  }

  "The api" should "return an error if user does not have reporting access" in withContainers { case _: PostgreSQLContainer =>
    val response: GraphqlQueryData = runTestQuery("query_no_filters", invalidReportingToken())
    response.errors.head.message should include("does not have permission to access")
  }

  "The api" should "return all requested fields correctly" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e96c7ed-6b56-4c1f-b02c-13c1d4f85c36")
    val fileId = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8d029dd4284b")

    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileId, "Antivirus", "Failure")
    utils.addAntivirusMetadata(fileId.toString, "virus")
    utils.addFFIDMetadata(fileId.toString)
    utils.addFileProperty(SHA256ClientSideChecksum)
    utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, SHA256ClientSideChecksum, "checksum")

    val response: GraphqlQueryData = runTestQuery("query_no_filters", validReportingToken("reporting"))
    
    val failure = response.data.get.getFileCheckFailures.head
    failure.fileId shouldBe fileId
    failure.consignmentId shouldBe consignmentId
    failure.consignmentType shouldBe "standard"
    failure.rankOverFilePath shouldBe 1
    failure.PUID should not be defined
    failure.userId shouldBe userId
    failure.statusType shouldBe "Antivirus"
    failure.statusValue shouldBe "Failure"
    failure.seriesName shouldBe Some("seriesName")
    failure.transferringBodyName shouldBe Some("transferringBodyName")
    failure.antivirusResult shouldBe Some("virus")
    failure.extension should not be defined
    failure.identificationBasis should not be defined
    failure.extensionMismatch shouldBe false
    failure.formatName should not be defined
    failure.checksum shouldBe Some("checksum")
  }

  "The api" should "filter file check failures by date range" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e96c7ed-6b56-4c1f-b02c-13c1d4f85c36")
    val fileOneId = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8d029dd4284b")
    val fileTwoId = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479")

    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)

    val withinRange = Instant.parse("2026-01-01T12:00:00Z")
    val outsideRange = Instant.parse("2026-01-02T12:00:00Z")

    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Antivirus", "Failure", Timestamp.from(withinRange))
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Antivirus", "Failure", Timestamp.from(outsideRange))

    utils.addAntivirusMetadata(fileOneId.toString, "virus")
    utils.addAntivirusMetadata(fileTwoId.toString, "virus")
    utils.addFFIDMetadata(fileOneId.toString)
    utils.addFFIDMetadata(fileTwoId.toString)

    val response: GraphqlQueryData = runTestQuery("query_filtered_by_date_range", validReportingToken("reporting"))

    response.data.get.getFileCheckFailures.size shouldBe 1
    response.data.get.getFileCheckFailures.head.fileId shouldBe fileOneId
  }
}