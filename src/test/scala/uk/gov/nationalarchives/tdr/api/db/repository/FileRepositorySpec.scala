package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
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

class FileRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  val folderOneId: UUID = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")
  val fileOneId: UUID = UUID.fromString("20e0676a-f0a1-4051-9540-e7df1344ac11")
  val fileTwoId: UUID = UUID.fromString("b5111f11-4dca-4f92-8239-505da567b9d0")

  "addFiles" should "create files and update ConsignmentStatus table for a consignment" in withContainers { case container: PostgreSQLContainer =>
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

  "countFilesInConsignment" should "return 0 if a consignment has no files" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c03fd4be-58c1-4cee-8d3c-d162bb4f7c02")

    utils.createConsignment(consignmentId, userId)

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId).futureValue

    consignmentFiles shouldBe 0
  }

  "countFilesInConsignment" should "return the total number of files in a consignment" in withContainers { case container: PostgreSQLContainer =>
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

  "countFilesInConsignment" should "return the total number of files and folders in a consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")
    val folderIdOne = UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9")
    val folderIdTwo = UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e")

    utils.createConsignment(consignmentId, userId)

    utils.createFile(folderIdOne, consignmentId, NodeType.directoryTypeIdentifier)
    utils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId, parentId = Some(folderIdOne))
    utils.createFile(folderIdTwo, consignmentId, NodeType.directoryTypeIdentifier)
    utils.createFile(UUID.fromString("317c7084-d3d4-435b-acd2-cfb317793843"), consignmentId, parentId = Some(folderIdTwo))

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId, fileTypeIdentifier = None).futureValue

    consignmentFiles shouldBe 4
  }

  "countFilesInConsignment" should "return the total number of folders in a consignment given a folder type filter" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")

    utils.createConsignment(consignmentId, userId)

    utils.createFile(UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9"), consignmentId, NodeType.directoryTypeIdentifier)
    utils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId)
    utils.createFile(UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e"), consignmentId)
    utils.createFile(UUID.fromString("1ad53749-aba4-4369-8fd6-2311111427cc"), consignmentId)

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId, fileTypeIdentifier = Some(NodeType.directoryTypeIdentifier)).futureValue

    consignmentFiles shouldBe 1
  }

  "countFilesInConsignment" should "return the total number of files in a consignment given a file type filter" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")

    utils.createConsignment(consignmentId, userId)

    utils.createFile(UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9"), consignmentId, NodeType.directoryTypeIdentifier)
    utils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId)
    utils.createFile(UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e"), consignmentId)
    utils.createFile(UUID.fromString("1ad53749-aba4-4369-8fd6-2311111427cc"), consignmentId)

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId, fileTypeIdentifier = Some(NodeType.fileTypeIdentifier)).futureValue

    consignmentFiles shouldBe 3
  }

  "countFilesInConsignment" should "return the total number of files and folders with the same parent given a parentId filter" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")
      val folderId = UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9")

      utils.createConsignment(consignmentId, userId)

      utils.createFile(folderId, consignmentId, NodeType.directoryTypeIdentifier)
      utils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId, parentId = Some(folderId))
      utils.createFile(UUID.fromString("1ad53749-aba4-4369-8fd6-2311111427cc"), consignmentId, NodeType.directoryTypeIdentifier, parentId = Some(folderId))
      utils.createFile(UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e"), consignmentId)

      val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId, parentId = Some(folderId), None).futureValue

      consignmentFiles shouldBe 2
  }

  "getFilesWithPassedAntivirus" should "return only files where the antivirus has found no virus" in withContainers { case container: PostgreSQLContainer =>
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

  "getConsignmentForFile" should "return the correct consignment for the given file id" in withContainers { case container: PostgreSQLContainer =>
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

  "getFiles" should "return files, file metadata and folders where no type filter applied" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getFiles(consignmentId, FileFilters(None)).futureValue
    files.size shouldBe 4
  }

  "getFiles" should "return files and file metadata only where 'file' type filter applied" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getFiles(consignmentId, FileFilters(Some(NodeType.fileTypeIdentifier))).futureValue
    files.size shouldBe 3
  }

  "getFiles" should "return folders only where 'folder' type filter applied" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getFiles(consignmentId, FileFilters(Some(NodeType.directoryTypeIdentifier))).futureValue
    files.size shouldBe 1
  }

  "getFiles" should "return files and file metadata where 'selectedFileIds' filter applied" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getFiles(consignmentId, FileFilters(selectedFileIds = Some(List(fileOneId, folderOneId)))).futureValue
    files.forall(p => List(fileOneId, folderOneId).contains(p._1.fileid)) shouldBe true
  }

  "getFiles" should "return files and file metadata for selected files where 'file' type and 'selectedFileIds' filters are applied" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getFiles(consignmentId, FileFilters(Some(NodeType.fileTypeIdentifier), Some(List(fileOneId)))).futureValue
      files.forall(_._1.fileid == fileOneId) shouldBe true
  }

  "getFileFields" should "return userId(s) associated with a given fileId" in withContainers { // Change this
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
      val fileIds = Seq(fileOneId, fileTwoId)

      val files = fileRepository.getFileFields(fileIds).futureValue

      files.size shouldBe(2)
      files.head._1 shouldBe(fileOneId)
      files(1)._1 shouldBe(fileTwoId)
      files.head._2 shouldBe (userId)
      files(1)._2 shouldBe(userId)
  }

  "getAllDescendants" should "return all descendants" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    val folderId = setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getAllDescendants(Seq(folderId)).futureValue
    files.size shouldBe 3
  }

  "getPaginatedFiles" should "return all files and folders after the cursor up to the limit value" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getPaginatedFiles(consignmentId, 2, 0, Some(fileOneId.toString), FileFilters()).futureValue
    files.size shouldBe 2
    files.head.fileid shouldBe fileOneId
    files.last.fileid shouldBe fileTwoId
  }

  "getPaginatedFiles" should "return only files when filter applied, after the cursor up to the limit value" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getPaginatedFiles(consignmentId, 2, 0, Some(fileOneId.toString), FileFilters(Some(NodeType.fileTypeIdentifier))).futureValue
    files.size shouldBe 2
    files.head.fileid shouldBe fileOneId
  }

  "getPaginatedFiles" should "return all files and folders up to limit where no cursor provided including first file" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getPaginatedFiles(consignmentId, 3, 0, None, FileFilters()).futureValue
    files.size shouldBe 3
    files.head.fileid shouldBe fileOneId
    files.last.fileid shouldBe folderOneId
  }

  "getPaginatedFiles" should "return no files where limit set at '0'" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    setUpFilesAndDirectories(consignmentId, utils)

    val files = fileRepository.getPaginatedFiles(consignmentId, 0, 2, None, FileFilters()).futureValue
    files.size shouldBe 0
  }

  "getPaginatedFiles" should "return files where non-existent cursor value provided, and filedId is greater than the cursor value" in withContainers {
    case container: PostgreSQLContainer =>
      val nonExistentFileId = "820e2eed-a979-4982-8627-26c8a0dcdb2d"
      val db = container.database
      val utils = TestUtils(db)
      val fileRepository = new FileRepository(db)
      val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
      setUpFilesAndDirectories(consignmentId, utils)

      val files = fileRepository.getPaginatedFiles(consignmentId, 2, 0, Some(nonExistentFileId), FileFilters()).futureValue
      files.size shouldBe 2
      files.head.fileid shouldBe fileOneId
      files.last.fileid shouldBe fileTwoId
  }

  "getPaginatedFiles" should "return no files where there are no files" in withContainers { case container: PostgreSQLContainer =>
    val nonExistentFileId = "820e2eed-a979-4982-8627-26c8a0dcdb2d"
    val db = container.database
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")

    val files = fileRepository.getPaginatedFiles(consignmentId, 2, 2, Some(nonExistentFileId), FileFilters()).futureValue
    files.size shouldBe 0
  }

  "getConsignmentParentFolder" should "return a parent folder for a consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")
    val parentFolderId = setUpFilesAndDirectories(consignmentId, utils)

    val parentFolder = fileRepository.getConsignmentParentFolder(consignmentId).futureValue
    parentFolder.size shouldBe 1
    parentFolder.head.fileid shouldBe parentFolderId
  }

  "getConsignmentParentFolder" should "not return a parent folder for a consignment which does not exist" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c6f78fef-704a-46a8-82c0-afa465199e66")

    val parentFolder = fileRepository.getConsignmentParentFolder(consignmentId).futureValue
    parentFolder.size shouldBe 0
  }

  "getConsignmentParentFolder" should "not return a parent folder if it does not exist for a valid consignment" in withContainers { case container: PostgreSQLContainer =>
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
    utils.createFile(folderOneId, consignmentId, NodeType.directoryTypeIdentifier, "folderName")
    utils.createFile(fileOneId, consignmentId, fileName = "FileName1", parentId = Some(folderOneId))
    utils.createFile(fileTwoId, consignmentId, fileName = "FileName2", parentId = Some(folderOneId))

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
