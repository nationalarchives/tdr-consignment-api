package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestDatabase
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import java.util.UUID
import scala.concurrent.ExecutionContext


class TransferringBodyRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "dbHasTransferringBodies" should "Should return true if db has 1 Transferring Bodies in it" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)

    addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")

    val dbHasTransferringBodies: Boolean = transferringBodyRepository.dbHasTransferringBodies.futureValue

    dbHasTransferringBodies shouldBe true
  }

  "dbHasTransferringBodies" should "Should return false if db has 0 Transferring Bodies in it" in {
    val db = DbConnection.db
    val transferringBodyRepository = new TransferringBodyRepository(db)

    val dbHasTransferringBodies: Boolean = transferringBodyRepository.dbHasTransferringBodies.futureValue

    dbHasTransferringBodies shouldBe false
  }
}
