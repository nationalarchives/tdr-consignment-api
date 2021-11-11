package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestDatabase
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.Tables.BodyRow

import java.util.UUID
import scala.concurrent.ExecutionContext

class TransferringBodyRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getTransferringBody" should "return the correct transferring body for the given series id" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)
    val seriesId = UUID.randomUUID()
    val bodyId = UUID.randomUUID()

    addTransferringBody(bodyId, "MOCK Department", "Code123")
    addSeries(seriesId, bodyId, "TDR-2020-XYZ")

    val tb: BodyRow = transferringBodyRepository.getTransferringBody(seriesId).futureValue
    tb.bodyid shouldBe bodyId
    tb.tdrcode shouldBe "Code123"
    tb.name shouldBe "MOCK Department"
  }

  "getTransferringBodyByCode" should "return the correct transferring body for the given code value" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)
    val bodyId = UUID.randomUUID()

    addTransferringBody(bodyId, "MOCK Department", "Code123")

    val tb: BodyRow = transferringBodyRepository.getTransferringBodyByCode("Code123").futureValue.get
    tb.bodyid shouldBe bodyId
    tb.tdrcode shouldBe "Code123"
    tb.name shouldBe "MOCK Department"
  }

  "dbHasTransferringBodies" should "return true if db has 1 Transferring Bodies in it" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)

    addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")

    val dbHasTransferringBodies: Boolean = transferringBodyRepository.dbHasTransferringBodies.futureValue

    dbHasTransferringBodies shouldBe true
  }

  "dbHasTransferringBodies" should "return false if db has 0 Transferring Bodies in it" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)

    val dbHasTransferringBodies: Boolean = transferringBodyRepository.dbHasTransferringBodies.futureValue

    dbHasTransferringBodies shouldBe false
  }
}
