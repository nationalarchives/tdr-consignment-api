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
  case class GraphqlMutationData(data: Option[updateConsignmentStatusUploadComplete], errors: List[GraphqlError] = Nil)
  class updateConsignmentStatusUploadComplete(updateConsignmentStatus: ConsignmentStatus)

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
    val token = validUserToken()

    createConsignment(consignmentId, userId)

    val expectedResponse = getDataFromFile[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("data_all")
    val response = runTestRequest[GraphqlMutationData](updateConsignmentStatusJsonFilePrefix)("mutation_data_all", token)
    response.data should equal(expectedResponse.data)
  }
}
