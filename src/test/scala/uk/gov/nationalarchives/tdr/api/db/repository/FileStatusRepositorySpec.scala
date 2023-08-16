package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ChecksumMatch, FFID}
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, TestContainerUtils, TestUtils}

import java.sql.Timestamp
import java.util.UUID

class FileStatusRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    super.afterContainersStart(containers)
  }

  val fileOneId: UUID = UUID.fromString("20e0676a-f0a1-4051-9540-e7df1344ac11")
  val fileTwoId: UUID = UUID.fromString("b5111f11-4dca-4f92-8239-505da567b9d0")

  "addFileStatuses" should "add all the given file status rows" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    val fileStatusRepository = new FileStatusRepository(db)
    utils.createConsignment(consignmentId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)

    val fileRow1 = AddFileStatusInput(fileOneId, FFID, "someFFIDStatus")
    val fileRow2 = AddFileStatusInput(fileTwoId, ChecksumMatch, "someChecksumMatch")
    val response = fileStatusRepository.addFileStatuses(List(fileRow1, fileRow2)).futureValue

    response.size shouldBe 2
    checkFileStatusExists(fileOneId, FFID, "someFFIDStatus", utils)
    checkFileStatusExists(fileTwoId, ChecksumMatch, "someChecksumMatch", utils)
  }

  "getFileStatus" should "return all the fileStatus rows for the consignment where no selected file ids provided" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type", "Value")

    val response = fileStatusRepository.getFileStatus(consignmentId, Set("Status Type")).futureValue
    response.size shouldBe 2
  }

  "getFileStatus" should "return only the fileStatus rows for the consignment for the selected file ids provided" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type", "Value")

    val response = fileStatusRepository.getFileStatus(consignmentId, Set("Status Type"), Some(Set(fileOneId))).futureValue
    response.size shouldBe 1
    response.head.fileid shouldBe fileOneId
  }

  "deleteFileStatus" should "delete the specified status type for the specified file" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type1", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type2", "Value")

    val response = fileStatusRepository.deleteFileStatus(Set(fileOneId), Set("Status Type1")).futureValue
    response shouldBe 1
    checkFileStatusDeleted(fileOneId, "Status Type1", utils)
    checkFileStatusExists(fileTwoId, "Status Type2", "Value", utils)
  }

  "deleteFileStatus" should "delete the specified status type for multiple files" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type1", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type1", "Value")

    val response = fileStatusRepository.deleteFileStatus(Set(fileOneId, fileTwoId), Set("Status Type1")).futureValue
    response shouldBe 2
    checkFileStatusDeleted(fileOneId, "Status Type1", utils)
    checkFileStatusDeleted(fileTwoId, "Status Type1", utils)
  }

  "deleteFileStatus" should "delete multiple status types for multiple files" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val consignmentId = UUID.fromString("f25fc436-12f1-48e8-8e1a-3fada106940a")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type1", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileOneId, "Status Type2", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type1", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Status Type2", "Value")
    utils.createFileStatusValues(UUID.randomUUID(), fileTwoId, "Different Status Type", "Value")

    val response = fileStatusRepository.deleteFileStatus(Set(fileOneId, fileTwoId), Set("Status Type1", "Status Type2")).futureValue
    response shouldBe 4
    checkFileStatusDeleted(fileOneId, "Status Type1", utils)
    checkFileStatusDeleted(fileTwoId, "Status Type1", utils)
    checkFileStatusDeleted(fileOneId, "Status Type2", utils)
    checkFileStatusDeleted(fileTwoId, "Status Type2", utils)

    checkFileStatusExists(fileTwoId, "Different Status Type", "Value", utils)
  }

  private def checkFileStatusExists(fileId: UUID, statusType: String, expectedValue: String, utils: TestUtils): Unit = {
    val rs = utils.getFileStatusResult(fileId, statusType)
    rs.contains(expectedValue) shouldBe true
  }

  private def checkFileStatusDeleted(fileId: UUID, statusType: String, utils: TestUtils): Unit = {
    val rs = utils.getFileStatusResult(fileId, statusType)
    rs.size shouldBe 0
  }
}
