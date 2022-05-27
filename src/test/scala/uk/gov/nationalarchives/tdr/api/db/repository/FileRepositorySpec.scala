package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class FileRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  val folderOneId = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")
  val fileOneId = UUID.fromString("20e0676a-f0a1-4051-9540-e7df1344ac11")
  val fileTwoId = UUID.fromString("b5111f11-4dca-4f92-8239-505da567b9d0")

  "addFiles" should "create files and update ConsignmentStatus table for a consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("94abafc4-165e-469b-ba93-eace3f224de5")
      val fileOneId = UUID.fromString("7499a278-2fec-4c47-92fb-dd9024c65d0d")
      val fileTwoId = UUID.fromString("e7d21444-0c62-4115-a4ad-320fd3d3dae3")
      val fileRows = Seq(
        FileRow(fileOneId, consignmentId, userId, Timestamp.from(Instant.now)),
        FileRow(fileTwoId, consignmentId, userId, Timestamp.from(Instant.now))
      )
      val consignmentStatusRow = ConsignmentstatusRow(
        UUID.fromString("ad5ac54c-6a67-4892-b8ac-120362df7917"),
        consignmentId,
        "Upload",
        "InProgress",
        Timestamp.from(Instant.now)
      )

      utils.createConsignment(consignmentId, userId)

      val addFiles: Seq[nationalarchives.Tables.FileRow] = fileRepository.addFiles(fileRows, consignmentStatusRow).futureValue

      addFiles.foreach { file =>
        file.consignmentid shouldBe consignmentId
        file.userid shouldBe userId
      }
      checkConsignmentStatusExists(consignmentId, utils)
  }

  "countFilesInConsignment" should "return 0 if a consignment has no files" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c03fd4be-58c1-4cee-8d3c-d162bb4f7c02")

      utils.createConsignment(consignmentId, userId)

      val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId).futureValue

      consignmentFiles shouldBe 0
  }

  "countFilesInConsignment" should "return the total number of files in a consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")

      utils.createConsignment(consignmentId, userId)

      utils.createFile(UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9"), consignmentId)
      utils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId)
      utils.createFile(UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e"), consignmentId)
      utils.createFile(UUID.fromString("1ad53749-aba4-4369-8fd6-2311111427cc"), consignmentId)

      val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId).futureValue

      consignmentFiles shouldBe 4
  }

  "countProcessedAvMetadataInConsignment" should "return 0 if consignment has no AVmetadata for files" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("64456c78-49bb-4bff-85c8-2fff053b9f8e")

      utils.createConsignment(consignmentId, userId)
      utils.createFile(UUID.fromString("2c2dedf5-56a1-497f-9f8a-19102739a055"), consignmentId)
      utils.createFile(UUID.fromString("c2d5c569-0fc4-4688-9523-157c4028f1b0"), consignmentId)
      utils.createFile(UUID.fromString("317c7084-d3d4-435b-acd2-cfb317793843"), consignmentId)
      //  We have created files, but not added any AVmetadata for those files to the AVMetadata repository.
      //  Thus, calling the countProcessedAvMetadataInConsignment method should return 0.

      val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(consignmentId).futureValue

      avMetadataFiles shouldBe 0
  }

  "countProcessedAvMetadataInConsignment" should "return the number of AVmetadata for files in a specified consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentOne = "049a11d7-06f5-4b11-b786-640000de76e1"
      val consignmentTwo = "dc2acd2c-3370-44fe-aeae-16aee30dc41f"
      val fileOneId = "5a102380-25ec-4881-a484-87d56fe9a0b3"
      val fileTwoId = "f616272d-4d80-44cf-90de-996c3984847c"
      val fileThreeId = "40c67a87-70c0-4424-b934-a395459ddbe0"

      //  Need to create a consignment with files in it
      utils.createConsignment(UUID.fromString(consignmentOne), userId)
      utils.createConsignment(UUID.fromString(consignmentTwo), userId)
      utils.createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentOne))
      utils.createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentOne))
      utils.createFile(UUID.fromString(fileThreeId), UUID.fromString(consignmentTwo))

      //  Then need to add data to the AVMetadata repository for these files
      utils.addAntivirusMetadata(fileOneId)
      utils.addAntivirusMetadata(fileTwoId)
      utils.addAntivirusMetadata(fileThreeId)

      val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(UUID.fromString(consignmentOne)).futureValue

      avMetadataFiles shouldBe 2
  }

  "countProcessedAvMetadataInConsignment" should "return number of AVmetadata rows with repetitive data filtered out" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = "c6f78fef-704a-46a8-82c0-afa465199e65"
      val fileOneId = "20e0676a-f0a1-4051-9540-e7df1344ac10"
      val fileTwoId = "b5111f11-4dca-4f92-8239-505da567b9df"

      utils.createConsignment(UUID.fromString(consignmentId), userId)
      utils.createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentId))
      utils.createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentId))

      (1 to 7).foreach { _ => utils.addAntivirusMetadata(fileOneId) }

      utils.addAntivirusMetadata(fileTwoId)
      val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(UUID.fromString(consignmentId)).futureValue

      avMetadataFiles shouldBe 2
  }

  "getFilesWithPassedAntivirus" should "return only files where the antivirus has found no virus" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("dba4515f-474c-4a5a-a297-260b6ba1ffa3")
      val fileOneId = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")
      val fileTwoId = UUID.fromString("53d2927e-89dd-48a8-bb19-d33b7baa4e44")

      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileOneId, consignmentId)
      utils.createFile(fileTwoId, consignmentId)

      utils.addAntivirusMetadata(fileOneId.toString, "")
      utils.addAntivirusMetadata(fileTwoId.toString)
      val files = fileRepository.getFilesWithPassedAntivirus(consignmentId).futureValue

      files.size shouldBe 1
      files.head.fileid shouldBe fileOneId
  }

  "getConsignmentForFile" should "return the correct consignment for the given file id" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("dba4515f-474c-4a5a-a297-260b6ba1ffa3")
      val fileOneId = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")

      utils.createConsignment(consignmentId, userId)
      utils.createFile(fileOneId, consignmentId)

      val files = fileRepository.getConsignmentForFile(fileOneId).futureValue

      files.size shouldBe 1
      files.head.consignmentid shouldBe consignmentId
  }

  "getFiles" should "return files, file metadata and folders where no type filter applied" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getFiles(consignmentId, FileFilters(None)).futureValue
      files.size shouldBe 4
  }

  "getFiles" should "return files and file metadata only where 'file' type filter applied" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getFiles(consignmentId, FileFilters(Some(NodeType.fileTypeIdentifier))).futureValue
      files.size shouldBe 3
  }

  "getFiles" should "return folders only where 'folder' type filter applied" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getFiles(consignmentId, FileFilters(Some(NodeType.directoryTypeIdentifier))).futureValue
      files.size shouldBe 1
  }

  "getAllDescendants" should "return all descendants" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      val folderId = setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getAllDescendants(Seq(folderId)).futureValue
      files.size shouldBe 3
  }

  "getPaginatedFiles" should "return all files and folders after the cursor up to the limit value" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, Some(fileOneId), FileFilters()).futureValue
      files.size shouldBe 2
      files.head.fileid shouldBe folderOneId
      files.last.fileid shouldBe fileTwoId
  }

  "getPaginatedFiles" should "return only files when filter applied, after the cursor up to the limit value" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, Some(fileOneId), FileFilters(Some(NodeType.fileTypeIdentifier))).futureValue
      files.size shouldBe 1
      files.head.fileid shouldBe fileTwoId
  }

  "getPaginatedFiles" should "return all files and folders up to limit where no cursor provided including first file" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, None, FileFilters()).futureValue
      files.size shouldBe 2
      files.head.fileid shouldBe fileOneId
      files.last.fileid shouldBe folderOneId
  }

  "getPaginatedFiles" should "return no files where limit set at '0'" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 0, None, FileFilters()).futureValue
      files.size shouldBe 0
  }

  "getPaginatedFiles" should "return files where non-existent cursor value provided, and filedId is greater than the cursor value" in withContainers {
    case container: PostgreSQLContainer =>
      val nonExistentFileId = UUID.fromString("820e2eed-a979-4982-8627-26c8a0dcdb2d")
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, Some(nonExistentFileId), FileFilters()).futureValue
      files.size shouldBe 2
      files.head.fileid shouldBe folderOneId
      files.last.fileid shouldBe fileTwoId
  }

  "getPaginatedFiles" should "return no files where there are no files" in withContainers {
    case container: PostgreSQLContainer =>
      val nonExistentFileId = UUID.fromString("820e2eed-a979-4982-8627-26c8a0dcdb2d")
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, Some(nonExistentFileId), FileFilters()).futureValue
      files.size shouldBe 0
  }

  "getConsignmentParentFolder" should "return a parent folder for a consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      val pareFolderId = setUpFilesAndDirectories(consignmentId, utils)

      val parentFolder = fileRepository.getConsignmentParentFolder(consignmentId).futureValue
      parentFolder.size shouldBe 1
      parentFolder.head.fileid shouldBe pareFolderId
  }

  "getConsignmentParentFolder" should "not return a parent folder for a consignment which does not exist" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")

      val parentFolder = fileRepository.getConsignmentParentFolder(consignmentId).futureValue
      parentFolder.size shouldBe 0
  }

  "getConsignmentParentFolder" should "not return a parent folder if it does not exist for a valid consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      utils.createConsignment(consignmentId, userId)

      val parentFolder = fileRepository.getConsignmentParentFolder(consignmentId).futureValue
      parentFolder.size shouldBe 0
  }

  private def setUpFilesAndDirectories(consignmentId: UUID, utils: TestUtils): UUID = {
    utils.createConsignment(consignmentId, userId)
    utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier)
    utils.createFile(fileOneId, consignmentId, parentId = Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId, parentId = Some(folderOneId))

    utils.addFileProperty("FilePropertyOne")
    utils.addFileProperty("FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "FilePropertyOne")
    utils.addFileMetadata(UUID.randomUUID().toString, fileOneId.toString, "FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileTwoId.toString, "FilePropertyOne")
    folderOneId
  }

  private def checkConsignmentStatusExists(consignmentId: UUID, utils: TestUtils): Unit = {
    val rs = utils.getConsignmentStatus(consignmentId, "Upload")
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    rs.next() should equal(false)
  }
}
