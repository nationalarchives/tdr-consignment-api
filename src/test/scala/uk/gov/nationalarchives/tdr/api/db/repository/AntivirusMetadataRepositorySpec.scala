package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import uk.gov.nationalarchives.Tables.AvmetadataRow
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

class AntivirusMetadataRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(60, Seconds))

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "addAntivirusMetadata" should "insert more rows than fit in one statement" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val repository = new AntivirusMetadataRepository(db)
    val consignmentId = UUID.fromString("c2efd3e6-6664-4582-8c28-dcf891f60e68")
    val fileIds = (1 to 11000).map(_ => UUID.randomUUID())
    utils.createConsignment(consignmentId, userId)
    utils.createFiles(fileIds, consignmentId)
    val now = Timestamp.from(Instant.now)
    val rows = fileIds.map(id => AvmetadataRow(id, "software", "1", "db", "", now)).toList

    repository.addAntivirusMetadata(rows).futureValue should equal(rows)

    utils.countRows("AVMetadata") should equal(rows.size)
  }

  "addAntivirusMetadata" should "return no rows for empty input" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database

    new AntivirusMetadataRepository(db).addAntivirusMetadata(Nil).futureValue should equal(Nil)
    TestUtils(db).countRows("AVMetadata") should equal(0)
  }
}
