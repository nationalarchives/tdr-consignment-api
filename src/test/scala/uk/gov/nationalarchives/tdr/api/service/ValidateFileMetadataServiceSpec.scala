package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{ClosureMetadata, DescriptiveMetadata}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource, TestDataHelper}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ValidateFileMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fixedTimeSource: FixedTimeSource.type = FixedTimeSource

  "validateAndAddAdditionalMetadataStatuses" should "not update 'additional metadata' statuses if properties to validate are not 'additional metadata' properties" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    testSetUp.stubMockResponses()

    val service = testSetUp.service
    val response = service.validateAdditionalMetadata(fileIds, Set("nonAdditionalMetadataProperty")).futureValue

    response.size shouldBe 0

    verify(testSetUp.mockFileStatusRepository, times(0)).deleteFileStatus(any[Set[UUID]], any[Set[String]])
    verify(testSetUp.mockFileStatusRepository, times(0)).addFileStatuses(any[List[AddFileStatusInput]])
  }

  "validateAndAddAdditionalMetadataStatuses" should "update 'additional metadata' statuses to 'Completed' for multiple files where there are no missing dependencies" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "some description", Timestamp.from(FixedTimeSource.now), userId, "description"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "someDate", Timestamp.from(FixedTimeSource.now), userId, "ClosurePeriod"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "30", Timestamp.from(FixedTimeSource.now), userId, "FoiExemptionCode"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "true", Timestamp.from(FixedTimeSource.now), userId, "TitleClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "alternative title", Timestamp.from(FixedTimeSource.now), userId, "TitleAlternate"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "false", Timestamp.from(FixedTimeSource.now), userId, "DescriptionClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "some description", Timestamp.from(FixedTimeSource.now), userId, "description"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "someDate", Timestamp.from(FixedTimeSource.now), userId, "ClosurePeriod"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "30", Timestamp.from(FixedTimeSource.now), userId, "FoiExemptionCode"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "true", Timestamp.from(FixedTimeSource.now), userId, "TitleClosed"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "alternative title", Timestamp.from(FixedTimeSource.now), userId, "TitleAlternate"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "false", Timestamp.from(FixedTimeSource.now), userId, "DescriptionClosed")
    )

    testSetUp.stubMockResponses(existingMetadataRows)

    val service = testSetUp.service

    val response = service.validateAdditionalMetadata(fileIds, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    val expectedAddFileStatusInput = convertFileStatusRowToAddFileStatusInput(response)

    verify(testSetUp.mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(testSetUp.mockFileStatusRepository, times(1)).addFileStatuses(expectedAddFileStatusInput)

    val file1Statuses = expectedAddFileStatusInput.filter(_.fileId == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statusType == ClosureMetadata).get
    file1ClosureStatus.statusValue should equal("Completed")
    val file1DescriptiveStatus = file1Statuses.find(_.statusType == DescriptiveMetadata).get
    file1DescriptiveStatus.statusValue should equal("Completed")

    val file2Statuses = expectedAddFileStatusInput.filter(_.fileId == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statusType == ClosureMetadata).get
    file2ClosureStatus.statusValue should equal("Completed")
    val file2DescriptiveStatus = file2Statuses.find(_.statusType == DescriptiveMetadata).get
    file2DescriptiveStatus.statusValue should equal("Completed")
  }

  "validateAndAddAdditionalMetadataStatuses" should "update 'ClosureMetadata' status to 'Incomplete' for multiple files where there are missing dependencies" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Closed", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "English", Timestamp.from(FixedTimeSource.now), userId, "Language"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "English", Timestamp.from(FixedTimeSource.now), userId, "Language")
    )

    testSetUp.stubMockResponses(existingMetadataRows)

    val service = testSetUp.service
    val response = service.validateAdditionalMetadata(fileIds, Set("ClosureType", "description")).futureValue

    val expectedAddFileStatusInput = convertFileStatusRowToAddFileStatusInput(response)

    expectedAddFileStatusInput.size shouldBe 4

    verify(testSetUp.mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(testSetUp.mockFileStatusRepository, times(1)).addFileStatuses(expectedAddFileStatusInput)

    val file1Statuses = expectedAddFileStatusInput.filter(_.fileId == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statusType == ClosureMetadata).get
    file1ClosureStatus.statusValue should equal("Incomplete")
    val file1DescriptiveStatus = file1Statuses.find(_.statusType == DescriptiveMetadata).get
    file1DescriptiveStatus.statusValue should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("Incomplete")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAndAddAdditionalMetadataStatuses" should "update 'additional metadata' statuses to 'NotEntered' for multiple files where all existing property values match defaults" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val userId = testSetUp.userId
    val fileId1 = testSetUp.fileId1
    val fileId2 = testSetUp.fileId2
    val fileIds = Set(fileId1, fileId2)

    val existingMetadataRows: List[FilemetadataRow] = List(
      FilemetadataRow(UUID.randomUUID(), fileId1, "Open", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "Open", Timestamp.from(FixedTimeSource.now), userId, "ClosureType"),
      FilemetadataRow(UUID.randomUUID(), fileId1, "English", Timestamp.from(FixedTimeSource.now), userId, "Language"),
      FilemetadataRow(UUID.randomUUID(), fileId2, "English", Timestamp.from(FixedTimeSource.now), userId, "Language")
    )

    testSetUp.stubMockResponses(existingMetadataRows)

    val service = testSetUp.service
    val response = service.validateAdditionalMetadata(fileIds, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    val expectedAddFileStatusInput = convertFileStatusRowToAddFileStatusInput(response)

    verify(testSetUp.mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(testSetUp.mockFileStatusRepository, times(1)).addFileStatuses(expectedAddFileStatusInput)

    val file1Statuses = response.filter(_.fileid == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("NotEntered")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("NotEntered")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "validateAndAddAdditionalMetadataStatuses" should "update 'additional metadata' statuses to 'NotEntered' for multiple files where there are no existing additional metadata properties" in {
    val testSetUp = new ValidatePropertiesSetUp()
    val fileIds = Set(testSetUp.fileId1, testSetUp.fileId2)
    testSetUp.stubMockResponses()

    val service = testSetUp.service
    val response = service.validateAdditionalMetadata(fileIds, Set("ClosureType", "description")).futureValue

    response.size shouldBe 4

    val expectedAddFileStatusInput = convertFileStatusRowToAddFileStatusInput(response)

    verify(testSetUp.mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(testSetUp.mockFileStatusRepository, times(1)).addFileStatuses(expectedAddFileStatusInput)

    val file1Statuses = response.filter(_.fileid == testSetUp.fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("NotEntered")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("NotEntered")

    val file2Statuses = response.filter(_.fileid == testSetUp.fileId2)
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("NotEntered")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  private class ValidatePropertiesSetUp {
    val userId: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()

    val mockCustomMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val mockFileMetadataRepository: FileMetadataRepository = mock[FileMetadataRepository]
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]
    val mockDisplayPropertiesService: DisplayPropertiesService = mock[DisplayPropertiesService]

    val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset
    val service = new ValidateFileMetadataService(mockCustomMetadataService, mockDisplayPropertiesService, mockFileMetadataRepository, mockFileStatusRepository)

    private def createExpectedFileStatusRow(fileId: UUID, statusType: String, statusValue: String): Seq[FilestatusRow] = {
      val mappedMetadataTypeValue = (statusValue, statusType) match {
        case ("Completed", _)                      => "Completed"
        case ("Incomplete", "ClosureMetadata")     => "Incomplete"
        case ("Incomplete", "DescriptiveMetadata") => "NotEntered"
        case ("Incomplete", _)                     => "Incomplete"
        case ("NotEntered", _)                     => "NotEntered"
      }
      Seq(FilestatusRow(UUID.randomUUID(), fileId, statusType, mappedMetadataTypeValue, Timestamp.from(FixedTimeSource.now)))
    }

    def stubMockResponses(metadataRows: List[FilemetadataRow] = List()): Unit = {
      def generateExpectedRows(input: List[AddFileStatusInput]): Future[Seq[FilestatusRow]] = {
        val expectedRows = input.flatMap { addStatusInput =>
          createExpectedFileStatusRow(addStatusInput.fileId, addStatusInput.statusType, addStatusInput.statusValue)
        }
        Future.successful(expectedRows)
      }

      when(mockFileStatusRepository.addFileStatuses(any[List[AddFileStatusInput]])).thenAnswer { invocation: InvocationOnMock =>
        val input: List[AddFileStatusInput] = invocation.getArgument(0)
        generateExpectedRows(input)
      }
      when(mockFileStatusRepository.deleteFileStatus(any[Set[UUID]], any[Set[String]])).thenReturn(Future.successful(1))
      when(mockCustomMetadataService.getCustomMetadata).thenReturn(Future(TestDataHelper.mockCustomMetadataFields()))
      when(mockCustomMetadataService.toAdditionalMetadataFieldGroups(any[Seq[CustomMetadataField]])).thenAnswer { invocation: InvocationOnMock =>
        val input: Seq[CustomMetadataField] = invocation.getArgument(0)
        new CustomMetadataPropertiesService(mock[CustomMetadataPropertiesRepository]).toAdditionalMetadataFieldGroups(input)
      }
      when(mockFileMetadataRepository.getFileMetadata(Some(any[UUID]), any[Option[Set[UUID]]], any[Option[Set[String]]])).thenReturn(Future(metadataRows))

      val expectedPropertyNames =
        Seq(
          "ClosureType",
          "TitleClosed",
          "DescriptionClosed",
          "FoiExemptionCode",
          "TitleAlternate",
          "AlternativeDescription",
          "Language",
          "description",
          "ClosurePeriod",
          "MultiValueWithDependencies"
        )
      when(mockDisplayPropertiesService.getActiveDisplayPropertyNames).thenReturn(Future(expectedPropertyNames))
    }
  }

  private def convertFileStatusRowToAddFileStatusInput(filestatusRows: List[FilestatusRow]): List[AddFileStatusInput] = {
    filestatusRows.map { filestatusRow =>
      AddFileStatusInput(filestatusRow.fileid, filestatusRow.statustype, filestatusRow.value)
    }
  }
}
