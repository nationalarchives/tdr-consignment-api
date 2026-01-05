package uk.gov.nationalarchives.tdr.api.db.repository

import cats.implicits.catsSyntaxOptionId
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.ConsignmentMetadataFilter
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import java.sql._
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class ConsignmentMetadataRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 60.seconds)

  private val consignmentMetadataProperty1 = "PublicRecordsConfirmed"
  private val consignmentMetadataProperty2 = "JudgmentType"

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "addConsignmentMetadata" should "add consignment metadata with the correct values" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    utils.addConsignmentProperty(consignmentMetadataProperty1)
    val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
    val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
    utils.createConsignment(consignmentId, userId)
    val input = Seq(ConsignmentmetadataRow(UUID.randomUUID(), consignmentId, consignmentMetadataProperty1, "value", Timestamp.from(Instant.now()), UUID.randomUUID()))
    val result = consignmentMetadataRepository.addConsignmentMetadata(input).futureValue.head
    result.propertyname should equal(consignmentMetadataProperty1)
    result.value should equal("value")
    checkMetadataAddedExists(consignmentId, db.source.createConnection())
  }

  "addConsignmentMetadata" should "batch and then add consignment metadata with the correct values" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    utils.addConsignmentProperty(consignmentMetadataProperty1)
    val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
    val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
    utils.createConsignment(consignmentId, userId)
    val inputs =
      (1 to 50000).map(_ => ConsignmentmetadataRow(UUID.randomUUID(), consignmentId, consignmentMetadataProperty1, "value", Timestamp.from(Instant.now()), UUID.randomUUID()))
    val result = consignmentMetadataRepository.addConsignmentMetadata(inputs).futureValue.head
    result.propertyname should equal(consignmentMetadataProperty1)
    result.value should equal("value")
    checkMetadataAddedExists(consignmentId, db.source.createConnection(), expectedResultSetSize = 50000)
  }

  "deleteConsignmentMetadata" should "delete consignment metadata rows for the given consignment id that have the specified property names" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      utils.addConsignmentProperty(consignmentMetadataProperty1)
      utils.addConsignmentProperty(consignmentMetadataProperty2)
      val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
      val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
      val consignmentFilters = ConsignmentMetadataFilter(List(consignmentMetadataProperty1, consignmentMetadataProperty2))
      utils.createConsignment(consignmentId, userId)
      utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, consignmentMetadataProperty1, "Yes")
      utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, consignmentMetadataProperty2, "Judgment")

      val result = consignmentMetadataRepository.deleteConsignmentMetadata(consignmentId, Set(consignmentMetadataProperty1, consignmentMetadataProperty2)).futureValue
      result should equal(2)
      val response = consignmentMetadataRepository.getConsignmentMetadata(consignmentId, consignmentFilters.some).futureValue
      response.isEmpty should equal(true)

  }

  "getConsignmentMetadata" should "return the correct metadata" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    utils.addConsignmentProperty(consignmentMetadataProperty1)
    val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
    val consignmentId = UUID.fromString("d511ecee-89ac-4643-b62d-76a41984a92b")
    utils.createConsignment(consignmentId, userId)
    utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, consignmentMetadataProperty1)
    val consignmentFilters = ConsignmentMetadataFilter(List(consignmentMetadataProperty1))
    val response = consignmentMetadataRepository.getConsignmentMetadata(consignmentId, consignmentFilters.some).futureValue.head
    response.value should equal("Result of ConsignmentMetadata processing")
    response.propertyname should equal(consignmentMetadataProperty1)
    response.consignmentid should equal(consignmentId)
  }

  def checkMetadataAddedExists(consignmentId: UUID, connection: Connection, expectedResultSetSize: Int = 1): Unit = {
    val sql = """SELECT * FROM "ConsignmentMetadata" WHERE "ConsignmentId" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    rs.getString("PropertyName") should equal(consignmentMetadataProperty1)
    rs.getString("Value") should equal("value")
    rs.last()
    rs.getRow should equal(expectedResultSetSize)
  }
}
