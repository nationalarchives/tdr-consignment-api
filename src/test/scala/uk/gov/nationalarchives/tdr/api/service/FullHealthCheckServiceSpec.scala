package uk.gov.nationalarchives.tdr.api.service

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestDatabase
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import java.util.UUID
import scala.concurrent.ExecutionContext


class FullHealthCheckServiceSpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "checkDbIsUpAndRunning" should "Should throw an exception if db has no Transferring Bodies in it" in {
    val db = DbConnection.db
    val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
    val thrownException = intercept[Exception] {
      fullHealthCheckService.checkDbIsUpAndRunning(db).futureValue
    }
    thrownException.getMessage should include("Health Check failed because there are no Transferring Bodies in the DB")
  }

  "checkDbIsUpAndRunning" should "Should return Unit if db has 1 or more Transferring Bodies in it" in {
    val db = DbConnection.db
    addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")
    val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
    val result: Unit = fullHealthCheckService.checkDbIsUpAndRunning(db).futureValue
    result shouldBe()
  }
}
