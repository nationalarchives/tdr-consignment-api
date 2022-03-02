package uk.gov.nationalarchives.tdr.api.service

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.mockito.MockitoSugar.mock
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.repository.TransferringBodyRepository
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt


class FullHealthCheckServiceSpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 60.seconds)

  "checkDbIsUpAndRunning" should "throw an exception if db has no Transferring Bodies in it" in withContainers {
    case containers: PostgreSQLContainer =>
      val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
      val thrownException = intercept[Exception] {
        mock[TransferringBodyRepository]
        fullHealthCheckService.checkDbIsUpAndRunning(containers.database).futureValue
      }
      thrownException.getMessage should include("Health Check failed because there are no Transferring Bodies in the DB")
  }

  "checkDbIsUpAndRunning" should "return Unit if db has 1 or more Transferring Bodies in it" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      utils.addTransferringBody(UUID.randomUUID(), "MOCK Department", "Code")
      val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
      val result: Unit = fullHealthCheckService.checkDbIsUpAndRunning(db).futureValue
      result shouldBe()
  }

  "checkDbIsUpAndRunning" should "throw an exception if the database is unavailable" in withContainers {
    case container: PostgreSQLContainer =>
    val testDb = container.database
    testDb.close()
    val fullHealthCheckService: FullHealthCheckService = new FullHealthCheckService()
    val thrownException: Exception = intercept[Exception] {
      fullHealthCheckService.checkDbIsUpAndRunning(testDb).futureValue
    }
    thrownException.getCause.getMessage should equal("Cannot initialize ExecutionContext; AsyncExecutor already shut down")
  }
}
