package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import uk.gov.nationalarchives.Tables.FilemetadataRow
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{AddFileMetadataInput, ClientSideFileSize}
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

class FileMetadataRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(2, Seconds))

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    super.afterContainersStart(containers)
  }

  private def checkFileMetadataExists(fileId: UUID, utils: TestUtils, numberOfFileMetadataRows: Int = 1): Assertion = {
    utils.countFileMetadata(fileId) should be(numberOfFileMetadataRows)
  }

  "addFileMetadata" should "add metadata with the correct values" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("306c526b-d099-470b-87c8-df7bd0aa225a")
    val fileId = UUID.fromString("ba176f90-f0fd-42ef-bb28-81ba3ffb6f05")
    utils.addFileProperty("FileProperty")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    val input = Seq(AddFileMetadataInput(fileId, "value", UUID.randomUUID(), "FileProperty"))
    val result = fileMetadataRepository.addFileMetadata(input).futureValue.head
    result.propertyname should equal("FileProperty")
    result.value should equal("value")
    checkFileMetadataExists(fileId, utils)
  }

  "addFileMetadata" should "add metadata multiple metadata rows and return the correct number" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("306c526b-d099-470b-87c8-df7bd0aa225a")
    val fileId = UUID.fromString("ba176f90-f0fd-42ef-bb28-81ba3ffb6f05")
    utils.addFileProperty("FileProperty")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    val numberOfFileMetadataRows = 100
    val input = (1 to numberOfFileMetadataRows).map(_ => AddFileMetadataInput(fileId, "value", UUID.randomUUID(), "FileProperty"))

    val result = fileMetadataRepository.addFileMetadata(input).futureValue

    result.length should equal(numberOfFileMetadataRows)
    checkFileMetadataExists(fileId, utils, numberOfFileMetadataRows)
  }

  "getFileMetadataByProperty" should "return the correct metadata" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileId = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    utils.addFileProperty("FileProperty")
    utils.addFileMetadata(UUID.randomUUID().toString, fileId.toString, "FileProperty")
    val response = fileMetadataRepository.getFileMetadataByProperty(fileId :: Nil, "FileProperty").futureValue.head
    response.value should equal("Result of FileMetadata processing")
    response.propertyname should equal("FileProperty")
    response.fileid should equal(fileId)
  }

  "getFileMetadata" should "x" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)

    utils.addFileProperty("FilePropertyOne")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "FilePropertyOne", value = "/a/file/path/filename1.docx")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, "FilePropertyOne", value = "/a/file/path/filename2.pdf")

    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId), None, Some(Set("FilePropertyOne")), Some("%/file%")).futureValue

    response.size should equal(0)
  }

  "getFileMetadata" should "return the correct metadata for the consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.addFileProperty("FilePropertyOne")
    utils.addFileProperty("FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "FilePropertyOne")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, "FilePropertyOne")

    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId)).futureValue

    response.length should equal(3)
    val filesMap: Map[UUID, Seq[FilemetadataRow]] = response.groupBy(_.fileid)
    val fileOneMetadata = filesMap.get(fileIdOne)
    val fileTwoMetadata = filesMap.get(fileIdTwo)
    val fileOneProperties = fileOneMetadata.get.groupBy(_.propertyname)
    fileOneProperties("FilePropertyOne").head.value should equal("Result of FileMetadata processing")
    fileOneProperties("FilePropertyTwo").head.value should equal("Result of FileMetadata processing")
    fileTwoMetadata.get.head.propertyname should equal("FilePropertyOne")
    fileTwoMetadata.get.head.value should equal("Result of FileMetadata processing")
  }

  "getFileMetadata" should "return the correct metadata for the given consignment and selected file ids" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.addFileProperty("FilePropertyOne")
    utils.addFileProperty("FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "FilePropertyOne")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, "FilePropertyOne")

    val selectedFileIds: Set[UUID] = Set(fileIdTwo)
    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(selectedFileIds)).futureValue

    response.length should equal(1)
    val filesMap: Map[UUID, Seq[FilemetadataRow]] = response.groupBy(_.fileid)
    val fileTwoMetadata = filesMap(fileIdTwo)

    fileTwoMetadata.head.propertyname should equal("FilePropertyOne")
    fileTwoMetadata.head.value should equal("Result of FileMetadata processing")
  }

  "getFileMetadata" should "return only the metadata rows of the files that have the specified property/properties" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    val filePropertyOne = "FilePropertyOne"
    val filePropertyTwo = "FilePropertyTwo"
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.addFileProperty(filePropertyOne)
    utils.addFileProperty("FilePropertyTwo")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyOne)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyTwo)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, filePropertyOne)

    val selectedFileIds: Set[UUID] = Set(fileIdOne, fileIdTwo)
    val response = fileMetadataRepository
      .getFileMetadata(
        Some(consignmentId),
        Some(selectedFileIds),
        Some(Set(filePropertyOne))
      )
      .futureValue

    response.length should equal(2)
    response.foreach(fileMetadataRow => List(fileIdOne, fileIdTwo).contains(fileMetadataRow.fileid))
  }

  "updateFileMetadataProperties" should "update the value and userId for the selected file ids only and return the number of rows it updated" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val fileMetadataRepository = new FileMetadataRepository(db)
      val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
      val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
      val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
      val fileIdThree = UUID.fromString("f2e8a105-a251-41ce-89a0-a03c2b321277")
      val fileIdFour = UUID.randomUUID()
      val userId2 = UUID.randomUUID()
      utils.createConsignment(consignmentId, userId)
      List(fileIdOne, fileIdTwo, fileIdThree, fileIdFour).foreach(utils.createFile(_, consignmentId))

      utils.addFileProperty("FilePropertyOne")
      utils.addFileProperty("FilePropertyTwo")
      utils.addFileProperty("FilePropertyThree")
      val metadataId1 = UUID.randomUUID()
      val metadataId2 = UUID.randomUUID()
      val metadataId3 = UUID.randomUUID()
      val metadataId4 = UUID.randomUUID()
      utils.addFileMetadata(metadataId1.toString, fileIdOne.toString, "FilePropertyOne")
      utils.addFileMetadata(metadataId2.toString, fileIdOne.toString, "FilePropertyTwo")
      utils.addFileMetadata(metadataId3.toString, fileIdTwo.toString, "FilePropertyOne")
      utils.addFileMetadata(metadataId4.toString, fileIdThree.toString, "FilePropertyThree")
      utils.addFileMetadata(UUID.randomUUID().toString, fileIdFour.toString, "FilePropertyOne", "test value")
      val newValue = "newValue"

      val updateResponse: Seq[Int] = fileMetadataRepository
        .updateFileMetadataProperties(
          Set(fileIdOne, fileIdTwo, fileIdThree),
          Map(
            "FilePropertyOne" -> FileMetadataUpdate(Seq(metadataId1, metadataId3), "FilePropertyOne", newValue, Timestamp.from(Instant.now()), userId2),
            "FilePropertyThree" -> FileMetadataUpdate(Seq(metadataId4), "FilePropertyThree", newValue, Timestamp.from(Instant.now()), userId2)
          )
        )
        .futureValue

      updateResponse should be(Seq(2, 1))

      val filePropertyUpdates = ExpectedFilePropertyUpdates(
        consignmentId = consignmentId,
        changedProperties = Map(fileIdOne -> Seq("FilePropertyOne"), fileIdTwo -> Seq("FilePropertyOne"), fileIdThree -> Seq("FilePropertyThree")),
        unchangedProperties = Map(fileIdOne -> Seq("FilePropertyTwo")),
        newValue = newValue,
        newUserId = userId2
      )

      checkCorrectMetadataPropertiesAdded(fileMetadataRepository: FileMetadataRepository, filePropertyUpdates: ExpectedFilePropertyUpdates)

      val fileMetadata = fileMetadataRepository.getFileMetadataByProperty(fileIdFour :: Nil, "FilePropertyOne").futureValue
      fileMetadata.head.value should equal("test value")
  }

  "updateFileMetadataProperties" should "return 0 if a file property that does not exist on the rows passed to it" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    val userId2 = UUID.randomUUID()
    utils.createConsignment(consignmentId, userId)
    List(fileIdOne, fileIdTwo).foreach(fileId => utils.createFile(fileId, consignmentId))
    utils.addFileProperty("FilePropertyOne")
    utils.addFileProperty("FilePropertyTwo")
    utils.addFileProperty("FilePropertyThree")
    val metadataId1 = UUID.randomUUID()
    val metadataId2 = UUID.randomUUID()
    val metadataId3 = UUID.randomUUID()
    utils.addFileMetadata(metadataId1.toString, fileIdOne.toString, "FilePropertyOne")
    utils.addFileMetadata(metadataId2.toString, fileIdOne.toString, "FilePropertyTwo")
    utils.addFileMetadata(metadataId3.toString, fileIdTwo.toString, "FilePropertyOne")
    val newValue = "newValue"

    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId)).futureValue

    val updateResponse = fileMetadataRepository
      .updateFileMetadataProperties(
        Set(fileIdOne, fileIdTwo),
        Map("NonExistentFileProperty" -> FileMetadataUpdate(Seq(metadataId1, metadataId3), "NonExistentFileProperty", newValue, Timestamp.from(Instant.now()), userId2))
      )
      .futureValue

    updateResponse should be(List(0))
    response.foreach(metadataRow => List("FilePropertyOne", "FilePropertyTwo").contains(metadataRow.propertyname))
  }

  "deleteFileMetadata" should "delete metadata rows for the selected files that have the specified property/properties" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val fileIdTwo = UUID.fromString("664f07a5-ab1d-4d66-abea-d97d81cd7bec")
    val filePropertyOne = "FilePropertyOne"
    val filePropertyTwo = "FilePropertyTwo"
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.addFileProperty(filePropertyOne)
    utils.addFileProperty(filePropertyTwo)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyOne)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyTwo)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, filePropertyOne)

    val selectedFileIds: Set[UUID] = Set(fileIdOne, fileIdTwo)

    val deleteResponse = fileMetadataRepository.deleteFileMetadata(selectedFileIds, Set(filePropertyOne)).futureValue
    deleteResponse should equal(2)

    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(selectedFileIds), Some(Set(filePropertyOne))).futureValue
    response.length should equal(0)

    val response2 = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(selectedFileIds), Some(Set(filePropertyTwo))).futureValue
    response2.length should equal(1)
  }

  "getSumOfFileSizes" should "return the sum of file sizes" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val fileIdThree = UUID.randomUUID()
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    utils.addFileProperty(ClientSideFileSize)
    List((fileIdOne, "1"), (fileIdTwo, "2"), (fileIdThree, "3")).foreach { case (id, size) =>
      utils.createFile(id, consignmentId)
      utils.addFileMetadata(UUID.randomUUID().toString, id.toString, ClientSideFileSize, size)
    }
    val repository = new FileMetadataRepository(container.database)
    val sum = repository.getSumOfFileSizes(consignmentId).futureValue
    sum should equal(6)
  }

  "getSumOfFileSizes" should "return zero if there is no metadata" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.randomUUID()
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    utils.createFile(UUID.randomUUID(), consignmentId)
    utils.createFile(UUID.randomUUID(), consignmentId)
    utils.createFile(UUID.randomUUID(), consignmentId)
    val repository = new FileMetadataRepository(container.database)
    val sum = repository.getSumOfFileSizes(consignmentId).futureValue
    sum should equal(0)
  }

  private def checkCorrectMetadataPropertiesAdded(fileMetadataRepository: FileMetadataRepository, filePropertyUpdates: ExpectedFilePropertyUpdates): Unit = {
    val response = fileMetadataRepository.getFileMetadata(Some(filePropertyUpdates.consignmentId)).futureValue
    val metadataRowById: Map[UUID, Seq[FilemetadataRow]] = response.groupBy(_.fileid)

    filePropertyUpdates.changedProperties.foreach { case (fileId, propertiesChanged) =>
      val fileMetadataForFile: Seq[FilemetadataRow] = metadataRowById.getOrElse(fileId, Seq())
      val fileMetadataForFileByProperty: Map[String, Seq[FilemetadataRow]] = fileMetadataForFile.groupBy(_.propertyname)
      propertiesChanged.foreach { property =>
        {
          val metadataRow: FilemetadataRow = fileMetadataForFileByProperty.getOrElse(property, Seq()).head
          metadataRow.value should equal(filePropertyUpdates.newValue)
          metadataRow.userid should equal(filePropertyUpdates.newUserId)
        }
      }
    }

    filePropertyUpdates.unchangedProperties.foreach { case (fileId, unchangedProperties) =>
      val fileMetadataForFile: Seq[FilemetadataRow] = metadataRowById.getOrElse(fileId, Seq())
      val fileMetadataForFileByProperty: Map[String, Seq[FilemetadataRow]] = fileMetadataForFile.groupBy(_.propertyname)
      unchangedProperties.foreach { property =>
        {
          val metadataRow: FilemetadataRow = fileMetadataForFileByProperty.getOrElse(property, Seq()).head
          metadataRow.value should equal("Result of FileMetadata processing")
          metadataRow.userid should equal(userId)
        }
      }
    }
  }

  case class ExpectedFilePropertyUpdates(
      consignmentId: UUID,
      changedProperties: Map[UUID, Seq[String]],
      unchangedProperties: Map[UUID, Seq[String]],
      newValue: String,
      newUserId: UUID
  )
}
