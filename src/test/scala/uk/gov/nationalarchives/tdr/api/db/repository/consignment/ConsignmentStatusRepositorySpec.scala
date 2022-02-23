package uk.gov.nationalarchives.tdr.api.db.repository.consignment

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant.now
import java.util.UUID
import scala.concurrent.ExecutionContext

class ConsignmentStatusRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  "addConsignmentStatus" should "add consignment status data" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("0292019d-d112-465b-b31e-72dfb4d1254d")
      val consignmentStatusId = UUID.fromString("d2f2c8d8-2e1d-4996-8ad2-b26ed547d1aa")
      val userId = UUID.fromString("7f7be445-9879-4514-8a3e-523cb9d9a188")
      val statusType = "Status"
      val statusValue = "Value"
      val createdTimestamp = Timestamp.from(now)

      utils.createConsignment(consignmentId, userId)
      val transferAgreementStatusRow = ConsignmentstatusRow(consignmentStatusId, consignmentId, statusType, statusValue, createdTimestamp)

      val consignmentStatus = consignmentStatusRepository.addConsignmentStatus(transferAgreementStatusRow).futureValue

      consignmentStatus.consignmentid should be(consignmentId)
      consignmentStatus.consignmentstatusid should be(consignmentStatusId)
      consignmentStatus.statustype should be(statusType)
      consignmentStatus.value should be(statusValue)
      convertTimestampToSimpleDate(consignmentStatus.createddatetime) should be(convertTimestampToSimpleDate(createdTimestamp))
  }

  "getConsignmentStatus" should "return all data from the consignment status" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
      val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")
      val statusType = "Upload"
      val statusValue = "InProgress"
      val createdTimestamp = Timestamp.from(now)

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusType, statusValue, createdTimestamp)

      val consignmentStatus = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue.head

      consignmentStatus.consignmentid should be(consignmentId)
      consignmentStatus.statustype should be(statusType)
      consignmentStatus.value should be(statusValue)
      convertTimestampToSimpleDate(consignmentStatus.createddatetime) should be(convertTimestampToSimpleDate(createdTimestamp))
      consignmentStatus.modifieddatetime should be(None)
  }

  "getConsignmentStatus" should "return an empty list if no consignment status rows are found matching a given consignmentId" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
      val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")

      utils.createConsignment(consignmentId, userId)

      val consignmentStatus = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

      consignmentStatus should be(empty)
  }

  "getConsignmentStatus" should "return all consignment statuses for a consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("b8271ba9-9ef4-4584-b074-5a48b2a34cec")
      val userId = UUID.fromString("aee2d1a9-e1db-43a0-9fd6-a6c342bb187b")
      val statusTypeOne = "TransferAgreement"
      val statusValueOne = "Complete"
      val statusTypeTwo = "Upload"
      val statusValueTwo = "Complete"
      val statusTypeThree = "Export"
      val statusValueThree = "InProgress"

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, statusTypeOne, statusValueOne)
      utils.createConsignmentStatus(consignmentId, statusTypeTwo, statusValueTwo)
      utils.createConsignmentStatus(consignmentId, statusTypeThree, statusValueThree)

      val consignmentStatuses = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

      consignmentStatuses.length should be(3)

      consignmentStatuses.head.statustype should be(statusTypeOne)
      consignmentStatuses(1).statustype should be(statusTypeTwo)
      consignmentStatuses(2).statustype should be(statusTypeThree)
  }

  "getConsignmentStatus" should "return only the consignment status for the consignment specified" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("2e998acd-6e87-4437-92a4-e4267194fe38")
      val consignmentIdTwo = UUID.fromString("1b0fd1d8-9213-448f-baf3-44c87fe1828b")
      val consignmentIdThree = UUID.fromString("77ce2eaa-6f16-4b3c-8ec5-b47c46bf8d63")
      val userId = UUID.fromString("7f7be445-9879-4514-8a3e-523cb9d9a188")
      val statusType = "Upload"
      val statusValue = "Completed"

      utils.createConsignment(consignmentId, userId, consignmentRef = "TEST-1")
      utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TEST-2")
      utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TEST-3")

      utils.createConsignmentStatus(consignmentId, statusType, statusValue)
      utils.createConsignmentStatus(consignmentIdTwo, statusType, statusValue)
      utils.createConsignmentStatus(consignmentIdThree, statusType, statusValue)

      val consignmentStatus = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

      consignmentStatus.length should be(1)
      consignmentStatus.head.consignmentid should be(consignmentId)
  }

  "updateConsignmentStatus" should "update a consignments' status value to 'completed'" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("2e998acd-6e87-4437-92a4-e4267194fe38")
      val userId = UUID.fromString("7f7be445-9879-4514-8a3e-523cb9d9a188")
      val statusType = "Upload"
      val statusValue = "Completed"
      val createdTimestamp = Timestamp.from(now)
      val modifiedTimestamp = Timestamp.from(now)

      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, "Upload", "InProgress", createdTimestamp)
      val response: Int =
        consignmentStatusRepository.updateConsignmentStatus(consignmentId, statusType, statusValue, modifiedTimestamp).futureValue

      val consignmentStatusRetrieved = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue.head

      response should be(1)
      consignmentStatusRetrieved.value should be(statusValue)
      consignmentStatusRetrieved.statustype should be(statusType)
      convertTimestampToSimpleDate(consignmentStatusRetrieved.modifieddatetime.get) should be(convertTimestampToSimpleDate(modifiedTimestamp))
  }

  "updateConsignmentStatus" should "only update the value of the status type passed in" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val utils = databaseUtils(container)
      val consignmentStatusRepository = new ConsignmentStatusRepository(db)
      val consignmentId = UUID.fromString("2e998acd-6e87-4437-92a4-e4267194fe38")
      val userId = UUID.fromString("7f7be445-9879-4514-8a3e-523cb9d9a188")
      val statusTypeOne = "TransferAgreement"
      val statusTypeTwo = "Upload"
      val statusTypeThree = "Export"
      val statusValueOne = "InProgress"
      val statusValueTwo = "InProgress"
      val statusValueThree = "InProgress"
      val newStatusValueOne = "Completed"
      val createdTimestamp = Timestamp.from(now)
      val modifiedTimestamp = Timestamp.from(now)

      utils.createConsignment(consignmentId, userId)

      utils.createConsignmentStatus(consignmentId, statusTypeOne, statusValueOne, createdTimestamp)
      utils.createConsignmentStatus(consignmentId, statusTypeTwo, statusValueTwo, createdTimestamp)
      utils.createConsignmentStatus(consignmentId, statusTypeThree, statusValueThree, createdTimestamp)

      val response: Int =
        consignmentStatusRepository.updateConsignmentStatus(consignmentId, statusTypeOne, newStatusValueOne, modifiedTimestamp).futureValue

      val consignmentStatusRetrieved = consignmentStatusRepository.getConsignmentStatus(consignmentId).futureValue

      response should be(1)
      consignmentStatusRetrieved.head.statustype should be(statusTypeOne)
      consignmentStatusRetrieved.head.value should be(newStatusValueOne)
      convertTimestampToSimpleDate(consignmentStatusRetrieved.head.modifieddatetime.get) should be(convertTimestampToSimpleDate(modifiedTimestamp))

      consignmentStatusRetrieved(1).statustype should be(statusTypeTwo)
      consignmentStatusRetrieved(1).value should be(statusValueTwo)
      convertTimestampToSimpleDate(consignmentStatusRetrieved(1).createddatetime) should be(convertTimestampToSimpleDate(createdTimestamp))

      consignmentStatusRetrieved(2).statustype should be(statusTypeThree)
      consignmentStatusRetrieved(2).value should be(statusValueThree)
      convertTimestampToSimpleDate(consignmentStatusRetrieved(2).createddatetime) should be(convertTimestampToSimpleDate(createdTimestamp))
  }

  private def convertTimestampToSimpleDate(timestamp: Timestamp): String = {
    val simpleDateFormat = new SimpleDateFormat()
    simpleDateFormat.format(timestamp)
  }
}
