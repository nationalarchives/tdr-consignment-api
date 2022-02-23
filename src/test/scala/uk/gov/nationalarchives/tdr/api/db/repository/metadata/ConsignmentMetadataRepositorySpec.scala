package uk.gov.nationalarchives.tdr.api.db.repository.metadata

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.Config
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentMetadataRepository
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import java.sql.{PreparedStatement, ResultSet, Timestamp, Types}
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class ConsignmentMetadataRepositorySpec extends TestContainerUtils with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  private val consignmentMetadataProperty = "AllEnglishConfirmed"

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  "addConsignmentMetadata" should "add consignment metadata with the correct values" in withContainers {
    case container: PostgreSQLContainer =>
      val appConfig = config(container)
      val utils = databaseUtils(container)
      utils.addConsignmentProperty(consignmentMetadataProperty)
      val db = DbConnection.db(appConfig)
      val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
      val consignmentId = UUID.fromString("d4c053c5-f83a-4547-aefe-878d496bc5d2")
      utils.createConsignment(consignmentId, userId)
      val input = Seq(ConsignmentmetadataRow(
        UUID.randomUUID(), consignmentId, consignmentMetadataProperty, "value", Timestamp.from(Instant.now()), UUID.randomUUID()))
      val result = consignmentMetadataRepository.addConsignmentMetadata(input).futureValue.head
      result.propertyname should equal(consignmentMetadataProperty)
      result.value should equal("value")
      checkMetadataAddedExists(consignmentId, appConfig)
  }

  "getConsignmentMetadata" should "return the correct metadata" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection.db(config(container))
      val utils = databaseUtils(container)
      val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
      val consignmentId = UUID.fromString("d511ecee-89ac-4643-b62d-76a41984a92b")
      utils.addConsignmentProperty(consignmentMetadataProperty)
      utils.createConsignment(consignmentId, userId)
      utils.addConsignmentMetadata(UUID.randomUUID(), consignmentId, consignmentMetadataProperty)
      val response = consignmentMetadataRepository.getConsignmentMetadata(consignmentId, consignmentMetadataProperty).futureValue.head
      response.value should equal("Result of ConsignmentMetadata processing")
      response.propertyname should equal(consignmentMetadataProperty)
      response.consignmentid should equal(consignmentId)
  }

  private def checkMetadataAddedExists(consignmentId: UUID, config: Config): Unit = {
    val sql = """SELECT * FROM "ConsignmentMetadata" WHERE "ConsignmentId" = ?"""
    val ps: PreparedStatement = DbConnection.db(config).source.createConnection().prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    rs.getString("PropertyName") should equal(consignmentMetadataProperty)
    rs.getString("Value") should equal("value")
    rs.next() should equal(false)
  }
}
