package uk.gov.nationalarchives.tdr.api.service

import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils

import java.util.UUID
import scala.concurrent.ExecutionContext


class FullHealthCheckServiceSpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  "checkDbIsUpAndRunning" should "throw an exception if db has no Transferring Bodies in it" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
      val thrownException = intercept[Exception] {
        fullHealthCheckService.checkDbIsUpAndRunning(db).futureValue
      }
      thrownException.getMessage should include("Health Check failed because there are no Transferring Bodies in the DB")
  }

  "checkDbIsUpAndRunning" should "return Unit if db has 1 or more Transferring Bodies in it" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      databaseUtils(container).addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")
      val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
      val result: Unit = fullHealthCheckService.checkDbIsUpAndRunning(db).futureValue
      result shouldBe()
  }

  "checkDbIsUpAndRunning" should "throw an exception if the database is unavailable" in {
    val testDb = SlickSession.forConfig("consignmentapi").db
    testDb.close()
    val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
    val thrownException: Exception = intercept[Exception] {
      fullHealthCheckService.checkDbIsUpAndRunning(testDb).futureValue
    }
    thrownException.getCause.getMessage should equal("Cannot initialize ExecutionContext; AsyncExecutor already shut down")
  }
}
