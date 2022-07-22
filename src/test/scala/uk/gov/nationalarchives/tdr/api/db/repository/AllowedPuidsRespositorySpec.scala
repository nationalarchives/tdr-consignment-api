package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import scala.concurrent.ExecutionContext

class AllowedPuidsRespositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "checkAllowedPuidExists" should "return 'true' if puid present" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val allowedPuidsRepository = new AllowedPuidsRepository(db)

      utils.createAllowedPuids("puid1", "description 1", "reason1")

      val result = allowedPuidsRepository.checkAllowedPuidExists("puid1").futureValue
      result shouldBe true
  }

  "checkAllowedPuidExists" should "return 'false' if puid not present" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val allowedPuidsRepository = new AllowedPuidsRepository(db)

      utils.createAllowedPuids("puid1", "description 1", "reason1")

      val result = allowedPuidsRepository.checkAllowedPuidExists("puid2").futureValue
      result shouldBe false
  }
}
