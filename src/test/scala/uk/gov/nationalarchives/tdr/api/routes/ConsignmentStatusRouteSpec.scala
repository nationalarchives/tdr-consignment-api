package uk.gov.nationalarchives.tdr.api.routes

import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
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

  case class ConsignmentStatus(
      consignmentStatusId: Option[UUID],
      consignmentId: Option[UUID],
      statusType: Option[String],
      value: Option[String],
      modifiedDatetime: Option[ZonedDateTime]
  )

  "addConsignmentStatus" should "add consignment status" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val token = validUserToken(userId)

    utils.createConsignment(consignmentId, userId)

    val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_all")
    val response = runAddConsignmentStatusTestMutation("mutation_data_all", token)

    response.data.get.addConsignmentStatus should equal(expectedResponse.data.get.addConsignmentStatus)
  }

  "addConsignmentStatus" should "return an error if the consignment statusType for the consignment already exists" in withContainers { case container: PostgreSQLContainer =>
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

  "addConsignmentStatus" should "return an error if a consignment that doesn't exist is queried" in withContainers { case container: PostgreSQLContainer =>
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val token = validUserToken(userId)

    val expectedResponse = expectedAddConsignmentStatusMutationResponse("data_invalid_consignmentid")
    val response = runAddConsignmentStatusTestMutation("mutation_invalid_consignmentid", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
    response.errors.head.extensions.get.code should equal("NOT_AUTHORISED")
  }

  "addConsignmentStatus" should "return an error if an invalid statusType is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "addConsignmentStatus" should "return an error if an invalid statusValue is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "addConsignmentStatus" should "return an error if an invalid statusType and statusValue is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "updateConsignmentStatus" should "update consignment status" in withContainers { case container: PostgreSQLContainer =>
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

  "updateConsignmentStatus" should "update statuses where override user id is present in input" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "Series"
    val statusValue = "InProgress"
    val token = validTransferServiceToken("data-load")

    utils.createConsignment(consignmentId, userId)
    utils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_all")
    val response = runUpdateConsignmentStatusTestMutation("mutation_override_user_id", token)

    response.data.get.updateConsignmentStatus should equal(expectedResponse.data.get.updateConsignmentStatus)
  }

  "updateConsignmentStatus" should "throw an error when the override user id does not match the consignment user id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "Series"
    val statusValue = "InProgress"
    val token = validTransferServiceToken("data-load")

    utils.createConsignment(consignmentId, userId)
    utils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val response = runUpdateConsignmentStatusTestMutation("mutation_incorrect_override_user_id", token)

    response.errors.head.message should equal("User 'c44f1b9b-1275-4bc3-831c-808c50a0222d' does not have access to consignment '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e'")
    response.data.get.updateConsignmentStatus shouldBe None
  }

  "updateConsignmentStatus" should "throw an error if user does not have permission to override user id" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "Series"
    val statusValue = "InProgress"
    val token = validTransferServiceToken("some-random-role")

    utils.createConsignment(consignmentId, userId)
    utils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val response = runUpdateConsignmentStatusTestMutation("mutation_incorrect_override_user_id", token)

    response.errors.head.message should equal("User '5be4be46-cbd3-4073-8500-3be04522145d' does not have access to consignment '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e'")
    response.data.get.updateConsignmentStatus shouldBe None
  }

  "updateConsignmentStatus" should "allow a transfer adviser user to update the consignment status" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "MetadataReview"
    val statusValue = "InProgress"
    val tnaUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
    val token = validTNAUserToken(userId = tnaUserId, tnaUserType = "transfer_adviser")

    utils.createConsignment(consignmentId, userId)
    utils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_all")
    val response = runUpdateConsignmentStatusTestMutation("mutation_metadata_review", token)

    response.data.get.updateConsignmentStatus should equal(expectedResponse.data.get.updateConsignmentStatus)
  }

  "updateConsignmentStatus" should "not allow a transfer adviser user to update the consignment status if the 'MetadataReview' status is no 'InProgress'" in withContainers {
    case container: PostgreSQLContainer =>
      val utils = TestUtils(container.database)
      val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
      val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
      val statusType = "MetadataReview"
      val statusValue = "CompletedWithIssues"
      val tnaUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
      val token = validTNAUserToken(tnaUserId)

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue)

      val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_not_owner")
      val response = runUpdateConsignmentStatusTestMutation("mutation_metadata_review", token)

      response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateConsignmentStatus" should "not allow a metadata viewer user to update the consignment status" in withContainers { case container: PostgreSQLContainer =>
    val utils = TestUtils(container.database)
    val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "MetadataReview"
    val statusValue = "InProgress"
    val tnaUserId = UUID.fromString("29f65c4e-0eb8-4719-afdb-ace1bcbae4b6")
    val token = validTNAUserToken(tnaUserId)

    utils.createConsignment(consignmentId, userId)
    utils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_not_owner")
    val response = runUpdateConsignmentStatusTestMutation("mutation_metadata_review", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
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

  "updateConsignmentStatus" should "return an error if a consignment that doesn't exist is queried" in withContainers { case container: PostgreSQLContainer =>
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val token = validUserToken(userId)

    val expectedResponse = expectedUpdateConsignmentStatusMutationResponse("data_invalid_consignmentid")
    val response = runUpdateConsignmentStatusTestMutation("mutation_invalid_consignmentid", token)

    response.errors.head.message should equal(expectedResponse.errors.head.message)
  }

  "updateConsignmentStatus" should "return an error if an invalid statusType is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "updateConsignmentStatus" should "return an error if an invalid statusValue is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "updateConsignmentStatus" should "return an error if an invalid statusType and statusValue is passed" in withContainers { case container: PostgreSQLContainer =>
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

  "updateConsignmentStatus" should "return an error if statusValue is missing and the statusType isn't 'Upload'" in withContainers { case container: PostgreSQLContainer =>
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

}
