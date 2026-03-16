package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import uk.gov.nationalarchives.Tables.MetadatareviewlogRow
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, TestContainerUtils, TestUtils}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.ExecutionContext

class MetadataReviewLogRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 60.seconds)

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "addLogEntry" should "add a metadata review log entry with the correct values" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
    utils.createConsignment(consignmentId, userId)

    val logId = UUID.randomUUID()
    val action = "MetadataReview"
    val eventTime = Timestamp.from(FixedTimeSource.now)
    val logRow = MetadatareviewlogRow(logId, consignmentId, userId, action, eventTime)

    val repository = new MetadataReviewLogRepository(db)
    val result = repository.addLogEntry(logRow).futureValue

    result.metadatareviewlogid should equal(logId)
    result.consignmentid should equal(consignmentId)
    result.userid should equal(userId)
    result.action should equal(action)
    result.eventtime should equal(eventTime)
  }

  "getEntriesByConsignmentId" should "return all log entries for the given consignment id" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val consignmentId = UUID.fromString("a511ecee-89ac-4643-b62d-76a41984a92b")
    val otherConsignmentId = UUID.fromString("b511ecee-89ac-4643-b62d-76a41984a92b")
    utils.createConsignment(consignmentId, userId)
    utils.createConsignment(otherConsignmentId, userId)

    val logId1 = UUID.randomUUID()
    val logId2 = UUID.randomUUID()
    val logId3 = UUID.randomUUID()

    utils.addMetadataReviewLog(logId1, consignmentId, userId, "Approve")
    utils.addMetadataReviewLog(logId2, consignmentId, userId, "Reject")
    utils.addMetadataReviewLog(logId3, otherConsignmentId, userId, "Approve")

    val repository = new MetadataReviewLogRepository(db)
    val results = repository.getEntriesByConsignmentId(consignmentId).futureValue

    results.size should equal(2)
    results.map(_.metadatareviewlogid) should contain allOf (logId1, logId2)
    results.map(_.consignmentid).distinct should equal(Seq(consignmentId))
    results.map(_.action) should contain allOf ("Approve", "Reject")
  }

  "getEntriesByConsignmentId" should "return an empty sequence if no entries exist for the consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val consignmentId = UUID.fromString("c511ecee-89ac-4643-b62d-76a41984a92b")
    utils.createConsignment(consignmentId, userId)

    val repository = new MetadataReviewLogRepository(db)
    val results = repository.getEntriesByConsignmentId(consignmentId).futureValue

    results should be(empty)
  }
}
