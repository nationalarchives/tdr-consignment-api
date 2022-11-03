package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import scala.concurrent.ExecutionContext

class DisallowedPuidsRespositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "getDisallowedPuid" should "return disallowed puid if the puid exists" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val disallowedPuidsRepository = new DisallowedPuidsRepository(db)

    utils.createDisallowedPuids("puid1", "description 1", "reason1")
    utils.createDisallowedPuids("puid2", "description 2", "reason2")

    val result = disallowedPuidsRepository.getDisallowedPuid("puid1").futureValue
    result.get.reason shouldBe "reason1"
    result.get.puid shouldBe "puid1"
    result.get.active shouldBe true
    result.get.`puid description` shouldBe "description 1"
  }

  "getDisallowedPuid" should "return 'None' if puid not present" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val disallowedPuidsRepository = new DisallowedPuidsRepository(db)

    utils.createDisallowedPuids("puid1", "description 1", "reason1")

    val result = disallowedPuidsRepository.getDisallowedPuid("puid2").futureValue
    result.isEmpty shouldBe true
  }

  "activeReasons" should "return all disallowed reasons that are active" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val disallowedPuidsRepository = new DisallowedPuidsRepository(db)

    utils.createDisallowedPuids("puid1", "description 1", "reason1")
    utils.createDisallowedPuids("puid2", "description 2", "reason2", false)
    utils.createDisallowedPuids("puid3", "description 3", "reason3")

    val result = disallowedPuidsRepository.activeReasons().futureValue
    result.size shouldBe 2
    result.contains("reason1") shouldBe true
    result.contains("reason3") shouldBe true
  }

  "activeReasons" should "only return distinct disallowed reasons that are active if more than one puid has the same reason" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val disallowedPuidsRepository = new DisallowedPuidsRepository(db)

      utils.createDisallowedPuids("puid1", "description 1", "reason1")
      utils.createDisallowedPuids("puid2", "description 2", "reason2", false)
      utils.createDisallowedPuids("puid3", "description 3", "reason1")

      val result = disallowedPuidsRepository.activeReasons().futureValue
      result.size shouldBe 1
      result.contains("reason1") shouldBe true
  }
}
