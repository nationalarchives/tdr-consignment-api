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
import scala.concurrent.Future

class FileMetadataRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(180, Seconds))

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    super.afterContainersStart(containers)
  }

  private def checkFileMetadataExists(fileId: UUID, utils: TestUtils, numberOfFileMetadataRows: Int = 1, propertyName: String = "FileProperty"): Assertion = {
    utils.countFileMetadata(fileId, propertyName) should be(numberOfFileMetadataRows)
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
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)
    val numberOfFileMetadataRows = 100

    val properties = (1 to numberOfFileMetadataRows).map(i => s"FileProperty$i")
    properties.foreach(prop => utils.addFileProperty(prop))

    val input = properties.map(prop => AddFileMetadataInput(fileId, s"value_$prop", UUID.randomUUID(), prop))

    val result = fileMetadataRepository.addFileMetadata(input).futureValue

    result.length should equal(numberOfFileMetadataRows)
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

  "deleteFileMetadata" should "delete metadata rows for the given file id that have the specified property/properties" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.fromString("4c935c42-502c-4b89-abce-2272584655e1")
    val fileIdOne = UUID.fromString("4d5a5a00-77b4-4a97-aa3f-a75f7b13f284")
    val filePropertyOne = "FilePropertyOne"
    val filePropertyTwo = "FilePropertyTwo"
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileIdOne, consignmentId)
    utils.addFileProperty(filePropertyOne)
    utils.addFileProperty(filePropertyTwo)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyOne)
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, filePropertyTwo)

    val deleteResponse = fileMetadataRepository.deleteFileMetadata(fileIdOne, Set(filePropertyOne)).futureValue
    deleteResponse should equal(1)

    val response = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(Set(fileIdOne)), Some(Set(filePropertyOne))).futureValue
    response.length should equal(0)

    val response2 = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(Set(fileIdOne)), Some(Set(filePropertyTwo))).futureValue
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

  "totalClosedRecords" should "return total number of closed records" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.randomUUID()
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val fileIdThree = UUID.randomUUID()
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.createFile(fileIdThree, consignmentId)
    utils.addFileProperty("ClosureType")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "ClosureType", "Closed")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, "ClosureType", "closed")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdThree.toString, "ClosureType", "Open")
    val repository = new FileMetadataRepository(container.database)
    val sum = repository.totalClosedRecords(consignmentId).futureValue
    sum should equal(2)
  }

  "totalClosedRecords" should "return zero number of closed records when all of the records are open" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.randomUUID()
    val utils = TestUtils(container.database)
    utils.createConsignment(consignmentId, userId)
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val fileIdThree = UUID.randomUUID()
    utils.createFile(fileIdOne, consignmentId)
    utils.createFile(fileIdTwo, consignmentId)
    utils.createFile(fileIdThree, consignmentId)
    utils.addFileProperty("ClosureType")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdOne.toString, "ClosureType", "Open")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdTwo.toString, "ClosureType", "Open")
    utils.addFileMetadata(UUID.randomUUID().toString, fileIdThree.toString, "ClosureType", "Open")
    val repository = new FileMetadataRepository(container.database)
    val sum = repository.totalClosedRecords(consignmentId).futureValue
    sum should equal(0)
  }

  "addOrUpdateFileMetadata" should "handle multiple files and properties using upsert to update existing values" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.randomUUID()
    val fileId1 = UUID.randomUUID()
    val fileId2 = UUID.randomUUID()
    utils.addFileProperty("Prop1")
    utils.addFileProperty("Prop2")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId1, consignmentId)
    utils.createFile(fileId2, consignmentId)
    // Insert initial metadata for both files
    val initialInput = Seq(
      AddFileMetadataInput(fileId1, "val1", userId, "Prop1"),
      AddFileMetadataInput(fileId2, "val2", userId, "Prop2")
    )
    fileMetadataRepository.addFileMetadata(initialInput).futureValue
    // Now update both
    val newInput = Seq(
      AddFileMetadataInput(fileId1, "new-value-1", userId, "Prop1"),
      AddFileMetadataInput(fileId2, "new-value-2", userId, "Prop2")
    )
    val result = fileMetadataRepository.addOrUpdateFileMetadata(newInput).futureValue
    result.length should equal(2)
    result.find(_.fileid == fileId1).get.value should equal("new-value-1")
    result.find(_.fileid == fileId2).get.value should equal("new-value-2")
    checkFileMetadataExists(fileId1, utils, 1, "Prop1")
    checkFileMetadataExists(fileId2, utils, 1, "Prop2")
  }

  "addOrUpdateFileMetadata" should "insert new metadata when no existing record exists" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    utils.addFileProperty("FileProperty")
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)

    val input = Seq(AddFileMetadataInput(fileId, "new_value", userId, "FileProperty"))
    val result = fileMetadataRepository.addOrUpdateFileMetadata(input).futureValue

    result.length should equal(1)
    result.head.value should equal("new_value")
    result.head.fileid should equal(fileId)
    result.head.propertyname should equal("FileProperty")
    checkFileMetadataExists(fileId, utils, 1)
  }

  "addOrUpdateFileMetadata" should "do nothing if input is empty" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val fileMetadataRepository = new FileMetadataRepository(db)
    val result = fileMetadataRepository.addOrUpdateFileMetadata(Seq.empty).futureValue
    result shouldBe empty
  }

  "addOrUpdateFileMetadata" should "handle multiple concurrent calls with many properties correctly" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileId, consignmentId)

    // Create 30 unique properties
    val numberOfProperties = 30
    val properties = (1 to numberOfProperties).map(i => s"TestProperty$i")
    properties.foreach(prop => utils.addFileProperty(prop))

    val initialInput = properties.map(prop => AddFileMetadataInput(fileId, s"initial_$prop", userId, prop))
    fileMetadataRepository.addFileMetadata(initialInput).futureValue

    val totalInitialCount = properties.map(prop => utils.countFileMetadata(fileId, prop)).sum
    totalInitialCount should equal(numberOfProperties)

    // Prepare 10 different update calls, each updating different subsets of properties
    val numberOfCalls = 10
    val updateCalls = (1 to numberOfCalls).map { callIndex =>
      // Each call updates some properties with new values and some with empty values (deletes)
      val updates = properties.zipWithIndex.map { case (prop, propIndex) =>
        val shouldUpdate = (propIndex + callIndex) % 3 == 0 // Update every 3rd property based on call
        val shouldDelete = (propIndex + callIndex) % 7 == 0 // Delete every 7th property based on call

        if (shouldDelete) {
          AddFileMetadataInput(fileId, "", userId, prop) // Empty value = delete
        } else if (shouldUpdate) {
          AddFileMetadataInput(fileId, s"updated_call${callIndex}_$prop", userId, prop)
        } else {
          AddFileMetadataInput(fileId, s"initial_$prop", userId, prop) // Keep existing
        }
      }
      updates
    }

    val parallelResults = Future
      .traverse(updateCalls) { updateInput =>
        fileMetadataRepository.addOrUpdateFileMetadata(updateInput)
      }
      .futureValue

    // Verify all calls completed successfully
    parallelResults.length should equal(numberOfCalls)

    // Check final state - count remaining rows for each property
    val finalCounts = properties.map(prop => utils.countFileMetadata(fileId, prop))
    val totalFinalCount = finalCounts.sum

    // Each property should have either 0 or 1 row (due to unique constraint)
    finalCounts.foreach(count => count should (equal(0) or equal(1)))

    totalFinalCount should be >= 20
    totalFinalCount should be <= numberOfProperties

    // Verify data integrity - check that remaining rows have correct values
    val remainingMetadata = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(Set(fileId))).futureValue
    remainingMetadata.length should equal(totalFinalCount)

    // All remaining rows should have non-empty values
    remainingMetadata.foreach { row =>
      row.value should not be ""
      row.fileid should equal(fileId)
      properties should contain(row.propertyname)
    }
  }

  "addOrUpdateFileMetadata" should "handle ten calls with 1000 files correctly updating or deleting 40 properties" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val consignmentId = UUID.randomUUID()
    utils.createConsignment(consignmentId, userId)

    val numberOfFiles = 10000
    val fileIds = (1 to numberOfFiles).map(_ => UUID.randomUUID())
    fileIds.foreach(fileId => utils.createFile(fileId, consignmentId))

    val numberOfProperties = 40
    val properties = (1 to numberOfProperties).map(i => s"Prop$i")
    properties.foreach(p => utils.createFileProperty(p, "description", "propertyType", "text", editable = true, multivalue = false, "group", "fullName"))

    // Initial insert for all files
    val initialInputs = fileIds.flatMap { fileId =>
      properties.map(p => AddFileMetadataInput(fileId, s"initial_$p", userId, p))
    }

    // Split into chunks of 1000 files and make separate calls as persistence lambda has limits
    val chunkSize = 1000
    val chunks = initialInputs.grouped(chunkSize).toSeq
    Future
      .traverse(chunks) { chunk =>
        fileMetadataRepository.addOrUpdateFileMetadata(chunk)
      }
      .futureValue

    val initialCount = utils.countAllFileMetadata()
    initialCount should equal(numberOfFiles * numberOfProperties)

    val numberOfSimultaneousClientCalls = numberOfFiles / 1000
    val filesPerCall = numberOfFiles / numberOfSimultaneousClientCalls
    val updateCalls = (0 until numberOfSimultaneousClientCalls).map { callIndex =>
      val startIndex = callIndex * filesPerCall
      val endIndex = if (callIndex == numberOfSimultaneousClientCalls - 1) numberOfFiles else (callIndex + 1) * filesPerCall
      val filesToUpdate = fileIds.slice(startIndex, endIndex)

      // Each call updates ALL properties for its subset of files
      filesToUpdate.flatMap { fileId =>
        properties.zipWithIndex.map { case (prop, propIndex) =>
          if (propIndex % 2 == 0) {
            AddFileMetadataInput(fileId, s"updated_${prop}_call$callIndex", userId, prop) // Update even properties
          } else {
            AddFileMetadataInput(fileId, "", userId, prop) // Set odd properties to empty string
          }
        }
      }
    }

    // Execute all update calls in parallel
    val parallelResults = Future
      .traverse(updateCalls) { updateInput =>
        fileMetadataRepository.addOrUpdateFileMetadata(updateInput)
      }
      .futureValue

    parallelResults.length should equal(numberOfSimultaneousClientCalls)

    val finalCount = utils.countAllFileMetadata()
    finalCount should equal(numberOfFiles * numberOfProperties / 2)

    // Verify data integrity for a sample of files
    val sampleFileIds = fileIds.take(10)
    val finalMetadata = fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(sampleFileIds.toSet)).futureValue
    finalMetadata.length should equal(10 * numberOfProperties / 2) // 10 files, numberOfProperties properties each
    finalMetadata.foreach { row =>
      row.value should startWith("updated_Prop")
    }
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
