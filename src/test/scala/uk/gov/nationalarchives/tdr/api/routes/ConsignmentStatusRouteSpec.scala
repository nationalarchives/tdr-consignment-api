package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.dimafeng.testcontainers.PostgreSQLContainer
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestRequest, TestUtils}

import java.time.ZonedDateTime
import java.util.UUID


class ConsignmentStatusRouteSpec extends TestContainerUtils with Matchers with TestRequest {
  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)
  private val addConsignmentStatusJsonFilePrefix: String = "json/addconsignmentstatus_"
  val runAddConsignmentStatusTestMutation: (String, OAuth2BearerToken) => AddConsignmentStatusGraphqlMutationData =
    runTestRequest[AddConsignmentStatusGraphqlMutationData](addConsignmentStatusJsonFilePrefix)
  val expectedAddConsignmentStatusMutationResponse: String => AddConsignmentStatusGraphqlMutationData =
    getDataFromFile[AddConsignmentStatusGraphqlMutationData](addConsignmentStatusJsonFilePrefix)

  private val updateConsignmentStatusJsonFilePrefix: String = "json/updateconsignmentstatus_"
  val runUpdateConsignmentStatusTestMutation: (String, OAuth2BearerToken) => UpdateConsignmentStatusGraphqlMutationData =
    runTestRequest[UpdateConsignmentStatusGraphqlMutationData](updateConsignmentStatusJsonFilePrefix)
  val expectedUpdateConsignmentStatusMutationResponse: String => UpdateConsignmentStatusGraphqlMutationData =
    getDataFromFile[UpdateConsignmentStatusGraphqlMutationData](updateConsignmentStatusJsonFilePrefix)

  implicit val customConfig: Configuration = Configuration.default.withDefaults

  case class AddConsignmentStatusGraphqlMutationData(data: Option[AddConsignmentStatusComplete], errors: List[GraphqlError] = Nil)
  case class UpdateConsignmentStatusGraphqlMutationData(data: Option[UpdateConsignmentStatusComplete], errors: List[GraphqlError] = Nil)

  case class AddConsignmentStatusComplete(addConsignmentStatus: ConsignmentStatus)
  case class UpdateConsignmentStatusComplete(updateConsignmentStatus: Option[Int])

  case class ConsignmentStatus(consignmentStatusId: Option[UUID],
                               consignmentId: Option[UUID],
                               statusType: Option[String],
                               value: Option[String],
                               modifiedDatetime: Option[ZonedDateTime])

  "addConsignmentStatus" should "add consignment status" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)

      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_all")
      val response = runAddConsignmentStatusTestMutation("mutation_data_all", token)

      response.data.get.addConsignmentStatus should equal(expectedResponse.data.get.addConsignmentStatus)
  }

  "addConsignmentStatus" should "return an error if the consignment statusType for the consignment already exists" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val statusType = "Upload"
      val statusValue = "InProgress"
      val token = validUserToken(userId)

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_statusType_already_exists")
      val response = runAddConsignmentStatusTestMutation("mutation_statusType_already_exists", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal("INVALID_CONSIGNMENT_STATE")
  }

  "addConsignmentStatus" should "not allow a user to add the consignment status of a consignment that they did not create" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      utils.createConsignment(consignmentId, userId)

      val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
      val token = validUserToken(wrongUserId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_not_owner")
      val response = runAddConsignmentStatusTestMutation("mutation_not_owner", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "addConsignmentStatus" should "return an error if a consignment that doesn't exist is queried" in withContainers {
    case container: PostgreSQLContainer =>
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_invalid_consignmentid")
      val response = runAddConsignmentStatusTestMutation("mutation_invalid_consignmentid", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "addConsignmentStatus" should "return an error if an invalid statusType is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_invalid_statustype")
      val response = runAddConsignmentStatusTestMutation("mutation_invalid_statustype", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "addConsignmentStatus" should "return an error if an invalid statusValue is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_invalid_statusvalue")
      val response = runAddConsignmentStatusTestMutation("mutation_invalid_statusvalue", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "addConsignmentStatus" should "return an error if an invalid statusType and statusValue is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_invalid_statustype_and_statusvalue")
      val response = runAddConsignmentStatusTestMutation("mutation_invalid_statustype_and_statusvalue", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "update consignment status" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val statusType = "Series"
      val statusValue = "InProgress"
      val token = validUserToken(userId)

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_all")
      val response = runUpdateConsignmentStatusTestMutation("mutation_data_all", token)

      response.data.get.updateConsignmentStatus should equal(expectedResponse.data.get.updateConsignmentStatus)
  }

  "updateConsignmentStatus" should "not allow a user to update the consignment status of a consignment that they did not create" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val statusType = "Upload"
      val statusValue = "InProgress"

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val wrongUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
      val token = validUserToken(wrongUserId)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_not_owner")
      val response = runUpdateConsignmentStatusTestMutation("mutation_not_owner", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateConsignmentStatus" should "return an error if a consignment that doesn't exist is queried" in withContainers {
    case container: PostgreSQLContainer =>
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_invalid_consignmentid")
      val response = runUpdateConsignmentStatusTestMutation("mutation_invalid_consignmentid", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateConsignmentStatus" should "return an error if an invalid statusType is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)

      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val statusType = "Upload"
      val statusValue = "InProgress"

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_invalid_statustype")
      val response = runUpdateConsignmentStatusTestMutation("mutation_invalid_statustype", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "return an error if an invalid statusValue is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val statusType = "Upload"
      val statusValue = "InProgress"
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_invalid_statusvalue")
      val response = runUpdateConsignmentStatusTestMutation("mutation_invalid_statusvalue", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "return an error if an invalid statusType and statusValue is passed" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val statusType = "Upload"
      val statusValue = "InProgress"
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_invalid_statustype_and_statusvalue")
      val response = runUpdateConsignmentStatusTestMutation("mutation_invalid_statustype_and_statusvalue", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "return an error if statusValue is missing and the statusType isn't 'Upload'" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      utils.createConsignment(consignmentId, userId)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_missing_statusvalue_not_allowed")
      val response = runUpdateConsignmentStatusTestMutation("mutation_missing_statusvalue_not_allowed", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "return an error if statusType is 'Upload' and no statusValue is passed, " +
    "where consignment has no files with 'Upload' statuses" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val fileId = UUID.fromString("411e232f-bd48-4159-8fc5-7f083d406d91")
      val statusType = "Series"
      val statusValue = "InProgress"
      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileId, consignmentId)

      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_no_files_uploaded")
      val response = runUpdateConsignmentStatusTestMutation("mutation_no_files_uploaded", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
      response.errors.head.extensions should equal(expectedResponse.errors.head.extensions)
  }

  "updateConsignmentStatus" should "update the consignment status if statusType is 'Upload' and no statusValue was passed in, " +
    "where all consignment files have 'Success' Upload statuses " +
    "and files have 'Success' Upload statuses" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val fileId1 = UUID.fromString("411e232f-bd48-4159-8fc5-7f083d406d91")
      val fileId2 = UUID.fromString("1a821f03-fa86-44e2-b89c-7dda588a42bd")

      val statusType = "Upload"
      val statusValue = "InProgress"

      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileId1, consignmentId)
      utils.createFile(fileId2, consignmentId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)
      utils.createFileStatusValues(UUID.randomUUID(), fileId1, statusType, "Success")
      utils.createFileStatusValues(UUID.randomUUID(), fileId2, statusType, "Success")

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_all")
      val response = runUpdateConsignmentStatusTestMutation("mutation_files_all_uploaded_successfully", token)

      response.data.get.updateConsignmentStatus should equal(expectedResponse.data.get.updateConsignmentStatus)
  }

  "updateConsignmentStatus" should "update the consignment status if statusType is 'Upload' and no statusValue was passed in, " +
    "where some consignment files have 'Success' Upload statuses" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val token = validUserToken(userId)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val fileId1 = UUID.fromString("411e232f-bd48-4159-8fc5-7f083d406d91")
      val fileId2 = UUID.fromString("1a821f03-fa86-44e2-b89c-7dda588a42bd")
      val statusType = "Upload"
      val statusValue = "InProgress"

      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileId1, consignmentId)
      utils.createFile(fileId2, consignmentId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)
      utils.createFileStatusValues(UUID.randomUUID(), fileId1, statusType, "Success")
      utils.createFileStatusValues(UUID.randomUUID(), fileId2, statusType, "Failed")

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_all")
      val response = runUpdateConsignmentStatusTestMutation("mutation_not_all_files_uploaded_successfully", token)

      response.data.get.updateConsignmentStatus should equal(expectedResponse.data.get.updateConsignmentStatus)
  }
}
