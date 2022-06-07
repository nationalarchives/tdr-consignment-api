package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

class FileStatusRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    super.afterContainersStart(containers)
  }

  val fileOneId: UUID = UUID.fromString("20e0676a-f0a1-4051-9540-e7df1344ac11")
  val fileTwoId: UUID = UUID.fromString("b5111f11-4dca-4f92-8239-505da567b9d0")

  "getFileStatus" should "return all the fileStatus rows for the consignment where no selected file ids provided" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileStatusRepository = new FileStatusRepository(db)
      val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileOneId, consignmentId)
      utils.createFile(fileTwoId, consignmentId)
      utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type", "Value")
      utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type", "Value")

      val response = fileStatusRepository.getFileStatus(consignmentId, "Status Type").futureValue
      response.size shouldBe 2
  }

  "getFileStatus" should "return only the fileStatus rows for the consignment for the selected file ids provided" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileStatusRepository = new FileStatusRepository(db)
      val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileOneId, consignmentId)
      utils.createFile(fileTwoId, consignmentId)
      utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type", "Value")
      utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type", "Value")

      val response = fileStatusRepository.getFileStatus(consignmentId, "Status Type", Some(Set(fileOneId))).futureValue
      response.size shouldBe 1
      response.head.fileid shouldBe fileOneId
  }
}
