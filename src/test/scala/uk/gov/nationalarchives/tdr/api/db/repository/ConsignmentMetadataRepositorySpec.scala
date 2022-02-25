package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import slick.jdbc.JdbcBackend
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestDatabase, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import java.sql._
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class ConsignmentMetadataRepositorySpec extends TestContainerUtils with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 60.seconds)

  private val consignmentMetadataProperty = "AllEnglishConfirmed"

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "addConsignmentMetadata" should "add consignment metadata with the correct values" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      utils.addConsignmentProperty(consignmentMetadataProperty)
      val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
      val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
      utils.createConsignment(consignmentId, userId)
      val input = Seq(ConsignmentmetadataRow(
        UUID.randomUUID(), consignmentId, consignmentMetadataProperty, "value", Timestamp.from(Instant.now()), UUID.randomUUID()))
      val result = consignmentMetadataRepository.addConsignmentMetadata(input).futureValue.head
      result.propertyname should equal(consignmentMetadataProperty)
      result.value should equal("value")
      checkMetadataAddedExists(consignmentId, db.source.createConnection())
  }

  "getConsignmentMetadata" should "return the correct metadata" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      utils.addConsignmentProperty(consignmentMetadataProperty)
      val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
      val consignmentId = UUID.fromString("d511ecee-89ac-4643-b62d-76a41984a92b")
      utils.createConsignment(consignmentId, userId)
      utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, consignmentMetadataProperty)
      val response = consignmentMetadataRepository.getConsignmentMetadata(consignmentId, consignmentMetadataProperty).futureValue.head
      response.value should equal("Result of ConsignmentMetadata processing")
      response.propertyname should equal(consignmentMetadataProperty)
      response.consignmentid should equal(consignmentId)
  }

  def checkMetadataAddedExists(consignmentId: UUID, connection: Connection): Unit = {
    val sql = """SELECT * FROM "ConsignmentMetadata" WHERE "ConsignmentId" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    rs.getString("PropertyName") should equal(consignmentMetadataProperty)
    rs.getString("Value") should equal("value")
    rs.next() should equal(false)
  }
}
