package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, TestDatabase, TestUtils}

import java.sql.Timestamp
import java.time.Duration
import java.time.Instant.now
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getConsignmentStatus" should "return all of a consignment's status markers" in {
    val db = DbConnection.db
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
    val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")
    val statusType = "Upload"
    val statusValue = "InProgress"

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createConsignmentStatus(consignmentId, statusType, statusValue)

    val consignmentStatus = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue.head

    consignmentStatus.statustype should be(statusType)
    consignmentStatus.value should be(statusValue)
  }

  "getConsignmentStatus" should "return an empty list if no consignment status rows are found matching a given consignmentId" in {
    val db = DbConnection.db
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
    val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")

    TestUtils.createConsignment(consignmentId, userId)

    val consignmentStatus = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

    consignmentStatus should be(empty)
  }

  "getConsignmentStatus" should "return all consignment statuses for a consignment" in {
    val db = DbConnection.db
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
    val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")
    val statusType1 = "TransferAgreement"
    val statusValue1 = "Complete"
    val statusType2 = "Upload"
    val statusValue2 = "Complete"
    val statusType3 = "Export"
    val statusValue3 = "InProgress"

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createConsignmentStatus(consignmentId, statusType1, statusValue1)
    TestUtils.createConsignmentStatus(consignmentId, statusType2, statusValue2)
    TestUtils.createConsignmentStatus(consignmentId, statusType3, statusValue3)

    val consignmentStatuses = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

    consignmentStatuses.length should be(3)

    consignmentStatuses(0).statustype should be(statusType1)
    consignmentStatuses(1).statustype should be(statusType2)
    consignmentStatuses(2).statustype should be(statusType3)
  }

  "updateConsignmentStatus" should "update a consignments' status value to 'completed'" in {
    val db = DbConnection.db
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val consignmentId = UUID.fromString("2e998acd-6e87-4437-92a4-e4267194fe38")
    val userId = UUID.fromString("7f7be445-9879-4514-8a3e-523cb9d9a188")
    val statusType = "Upload"
    val statusValue = "Completed"
    val createdTimestamp = Timestamp.from(now)
    val modifiedTimestamp = Timestamp.from(now)

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createConsignmentStatus(consignmentId, "Upload", "InProgress", createdTimestamp)
    val response: Int =
      consignmentStatusRepository.updateConsignmentStatus(consignmentId, statusType, statusValue, modifiedTimestamp).futureValue

    val consignmentStatusRetrieved = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue.head

    response should be(1)
    consignmentStatusRetrieved.value should be(statusValue)
    consignmentStatusRetrieved.statustype should be(statusType)
    consignmentStatusRetrieved.modifieddatetime.get should be(modifiedTimestamp)
  }
}
