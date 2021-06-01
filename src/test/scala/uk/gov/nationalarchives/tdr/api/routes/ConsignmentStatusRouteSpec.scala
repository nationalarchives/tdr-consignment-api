package uk.gov.nationalarchives.tdr.api.routes

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestRequest}

import java.time.ZonedDateTime
import java.util.UUID


class ConsignmentStatusRouteSpec extends AnyFlatSpec with Matchers with TestRequest with TestDatabase {
  private val updateConsignmentStatusJsonFilePrefix: String = "json/updateconsignmentstatus_"
  private val transferringBodyId = UUID.fromString("4da472a5-16b3-4521-a630-5917a0722359")
  private val transferringBodyCode = "default-transferring-body-code"

  implicit val customConfig: Configuration = Configuration.default.withDefaults
  case class GraphqlMutationData(data: Option[UpdateConsignmentStatusUploadComplete], errors: List[GraphqlError] = Nil)
  case class UpdateConsignmentStatusUploadComplete(updateConsignmentStatusUploadComplete: Option[Int])

  case class ConsignmentStatus(consignmentStatusId: Option[UUID],
                               consignmentId: Option[UUID],
                               statusType: Option[String],
                               value: Option[String],
                               createdDatetime: Option[ZonedDateTime],
                               modifiedDatetime: Option[ZonedDateTime]
                              )

  val runTestMutation: (String, OAuth2BearerToken) => GraphqlMutationData = runTestRequest[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)
  val expectedMutationResponse: String => GraphqlMutationData = getDataFromFile[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)

  override def beforeEach(): Unit = {
    super.beforeEach()

    addTransferringBody(transferringBodyId, "Default transferring body name", transferringBodyCode)
  }

  "updateConsignmentStatusUploadComplete" should "update consignment status" in {
    val consignmentId = UUID.fromString("a8dc972d-58f9-4733-8bb2-4254b89a35f2")
    val userId = UUID.fromString("49762121-4425-4dc4-9194-98f72e04d52e")
    val statusType = "Upload"
    val statusValue = "InProgress"
    val token = validUserToken(userId)

    createConsignment(consignmentId, userId)
    createConsignmentUploadStatus(consignmentId, statusType, statusValue)

    val expectedResponse = getDataFromFile[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("data_all")
    val response = runTestRequest[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("mutation_data_all", token)

    response.data.get.updateConsignmentStatusUploadComplete should equal(expectedResponse.data.get.updateConsignmentStatusUploadComplete)
  }

  "updateConsignmentStatusUploadComplete" should "return an error if a consignment that doesn't exist is queried" in {
    val userId = UUID.fromString("dfee3d4f-3bb1-492e-9c85-7db1685ab12f")
    val token = validUserToken(userId)

    val expectedResponse = getDataFromFile[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("data_no_consignment")
    val response = runTestRequest[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("mutation_no_consignment", token)

    response.data.get.updateConsignmentStatusUploadComplete should equal(expectedResponse.data.get.updateConsignmentStatusUploadComplete)
  }
}


