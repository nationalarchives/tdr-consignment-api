package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues, Defined, Supplied, Text}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{AddOrUpdateFileMetadata, AddOrUpdateMetadata}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource, TestDataHelper}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with BeforeAndAfterEach {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val displayPropertiesServiceMock: DisplayPropertiesService = mock[DisplayPropertiesService]
  val customMetadataServiceMock: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val consignmentId: UUID = UUID.randomUUID()
  val fixedUuidSource = new FixedUUIDSource()

  override def beforeEach(): Unit = {
    reset(fileStatusRepositoryMock)
  }

  def mockResponse(statusTypes: Set[String], rows: Seq[FilestatusRow]): ScalaOngoingStubbing[Future[Seq[FilestatusRow]]] =
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, statusTypes)).thenReturn(Future(rows))

  def fileStatusRow(statusType: String, value: String): FilestatusRow =
    FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), statusType, value, Timestamp.from(Instant.now))

  "addFileStatus" should "add a file status row in the db" in {

    val fileStatusCaptor: ArgumentCaptor[List[AddFileStatusInput]] = ArgumentCaptor.forClass(classOf[List[AddFileStatusInput]])

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Upload", "Success")
    val repositoryReturnValue = Future(Seq(FilestatusRow(UUID.randomUUID(), addFileStatusInput.fileId, "Upload", "Success", Timestamp.from(Instant.now()))))
    when(fileStatusRepositoryMock.addFileStatuses(fileStatusCaptor.capture())).thenReturn(repositoryReturnValue)

    val response = createFileStatusService().addFileStatuses(AddMultipleFileStatusesInput(addFileStatusInput :: Nil)).futureValue.head

    val fileStatusRowActual = fileStatusCaptor.getValue.head
    fileStatusRowActual.statusType should equal(addFileStatusInput.statusType)
    fileStatusRowActual.statusValue should equal(addFileStatusInput.statusValue)
    fileStatusRowActual.fileId should equal(addFileStatusInput.fileId)
    response.fileId should equal(addFileStatusInput.fileId)
    response.statusValue should equal(addFileStatusInput.statusValue)
    response.statusType should equal(addFileStatusInput.statusType)
  }

  "addAdditionalMetadataStatuses" should "update 'additional metadata' statuses to 'NotEntered' or 'Completed' for multiple files" in {
    val testSetUp = new FileStatusServiceTestSetUp()
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
    val fileMetadataList: Seq[AddOrUpdateFileMetadata] = List(
      AddOrUpdateFileMetadata(
        testSetUp.fileId1,
        Seq(
          AddOrUpdateMetadata("ClosurePeriod", "40"),
          AddOrUpdateMetadata("ClosureType", "Closed"),
          AddOrUpdateMetadata("Language", "English"),
          AddOrUpdateMetadata("AlternativeDescription", ""),
          AddOrUpdateMetadata("TitleAlternate", ""),
          AddOrUpdateMetadata("FoiExemptionCode", "40"),
          AddOrUpdateMetadata("TitleClosed", "false"),
          AddOrUpdateMetadata("DescriptionClosed", "false"),
          AddOrUpdateMetadata("description", "eeeee")
        )
      ),
      AddOrUpdateFileMetadata(
        testSetUp.fileId2,
        Seq(
          AddOrUpdateMetadata("ClosurePeriod", ""),
          AddOrUpdateMetadata("ClosureType", "Open"),
          AddOrUpdateMetadata("Language", "English"),
          AddOrUpdateMetadata("AlternativeDescription", ""),
          AddOrUpdateMetadata("TitleAlternate", ""),
          AddOrUpdateMetadata("FoiExemptionCode", ""),
          AddOrUpdateMetadata("TitleClosed", "false"),
          AddOrUpdateMetadata("DescriptionClosed", "false"),
          AddOrUpdateMetadata("description", "")
        )
      )
    )

    val service = testSetUp.service
    val response = service.addAdditionalMetadataStatuses(fileMetadataList).futureValue

    response.size shouldBe 4

    val expectedAddFileStatusInput = convertFileStatusRowToAddFileStatusInput(response.toList)

    verify(testSetUp.mockFileStatusRepository, times(1)).deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
    verify(testSetUp.mockFileStatusRepository, times(1)).addFileStatuses(expectedAddFileStatusInput)

    val file1Statuses = response.filter(_.fileid == fileId1)
    file1Statuses.size shouldBe 2
    val file1ClosureStatus = file1Statuses.find(_.statustype == ClosureMetadata).get
    file1ClosureStatus.value should equal("Completed")
    val file1DescriptiveStatus = file1Statuses.find(_.statustype == DescriptiveMetadata).get
    file1DescriptiveStatus.value should equal("Completed")

    val file2Statuses = response.filter(_.fileid == fileId2)
    file2Statuses.size shouldBe 2
    val file2ClosureStatus = file2Statuses.find(_.statustype == ClosureMetadata).get
    file2ClosureStatus.value should equal("NotEntered")
    val file2DescriptiveStatus = file2Statuses.find(_.statustype == DescriptiveMetadata).get
    file2DescriptiveStatus.value should equal("NotEntered")
  }

  "allChecksSucceeded" should "return true if the checksum match, antivirus, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success), fileStatusRow(Redaction, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum match status is 'Mismatch' and the antivirus, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success), fileStatusRow(Redaction, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected' and the checksum, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(Antivirus, VirusDetected), fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, Success), fileStatusRow(Redaction, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows, the checksum match and ffid statuses are 'Success' and the redacted status is success" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, Success), fileStatusRow(Redaction, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum match file status rows, the antivirus and ffid statuses are 'Success' and the redaction status is success" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no ffid file status rows, the antivirus and checksum match statuses are 'Success' and the redaction status is success" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success), fileStatusRow(Redaction, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure " +
    "and multiple successful antivirus, ffid and redaction rows" in {
      mockResponse(
        Set(ChecksumMatch, Antivirus, FFID, Redaction),
        Seq(
          fileStatusRow(ChecksumMatch, Mismatch),
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(FFID, Success),
          fileStatusRow(FFID, Success),
          fileStatusRow(Redaction, Success),
          fileStatusRow(Redaction, Success)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure, " +
    "ffid success and multiple successful antivirus and redaction rows" in {
      mockResponse(
        Set(ChecksumMatch, Antivirus, FFID, Redaction),
        Seq(
          fileStatusRow(ChecksumMatch, Mismatch),
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(FFID, Success),
          fileStatusRow(Redaction, Success),
          fileStatusRow(Redaction, Success)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure " +
    "and multiple successful checksum match, ffid and redaction rows" in {
      mockResponse(
        Set(ChecksumMatch, Antivirus, FFID, Redaction),
        Seq(
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(Antivirus, VirusDetected),
          fileStatusRow(FFID, Success),
          fileStatusRow(FFID, Success)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure, " +
    "ffid success and multiple successful checksum and redaction matches" in {
      mockResponse(
        Set(ChecksumMatch, Antivirus, FFID, Redaction),
        Seq(
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(ChecksumMatch, Success),
          fileStatusRow(Antivirus, Success),
          fileStatusRow(Antivirus, VirusDetected),
          fileStatusRow(FFID, Success),
          fileStatusRow(Redaction, Success),
          fileStatusRow(Redaction, Success)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return true if there are multiple ffid success rows and multiple successful checksum match, antivirus and redaction rows" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, Success),
        fileStatusRow(Redaction, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if there are missing original files with a redacted file" in {
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID, Redaction),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(FFID, Success),
        fileStatusRow(Redaction, "MissingOriginalFile")
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "getFileStatus" should "return a Map Consisting of a FileId key and status value" in {
    mockResponse(Set(FFID), Seq(FilestatusRow(UUID.randomUUID(), consignmentId, FFID, Success, Timestamp.from(Instant.now))))
    val response = createFileStatusService().getFileStatus(consignmentId).futureValue
    val expected = Map(consignmentId -> Success)
    response should equal(expected)
  }

  "getFileStatuses" should "return expected file status types" in {
    val fileId1 = UUID.randomUUID()
    val fileId2 = UUID.randomUUID()

    mockResponse(
      Set(FFID, Upload, Antivirus),
      Seq(
        FilestatusRow(UUID.randomUUID(), fileId1, FFID, Success, Timestamp.from(Instant.now)),
        FilestatusRow(UUID.randomUUID(), fileId2, Upload, Success, Timestamp.from(Instant.now)),
        FilestatusRow(UUID.randomUUID(), fileId1, Antivirus, VirusDetected, Timestamp.from(Instant.now))
      )
    )

    val response = createFileStatusService().getFileStatuses(consignmentId, Set(FFID, Upload, Antivirus)).futureValue
    response.size shouldBe 3
    val statusFFID = response.find(_.statusType == FFID).get
    statusFFID.fileId should equal(fileId1)
    statusFFID.statusType should equal(FFID)
    statusFFID.statusValue should equal(Success)

    val statusUpload = response.find(_.statusType == Upload).get
    statusUpload.fileId should equal(fileId2)
    statusUpload.statusType should equal(Upload)
    statusUpload.statusValue should equal(Success)

    val statusAntivirus = response.find(_.statusType == Antivirus).get
    statusAntivirus.fileId should equal(fileId1)
    statusAntivirus.statusType should equal(Antivirus)
    statusAntivirus.statusValue should equal(VirusDetected)
  }

  "getFileStatuses" should "return empty status list if no statuses present" in {
    mockResponse(
      Set(FFID, Upload, Antivirus),
      Seq()
    )

    val response = createFileStatusService().getFileStatuses(consignmentId, Set(FFID, Upload, Antivirus)).futureValue
    response.size shouldBe 0
  }

  "allFileStatusTypes" should "include all file status types" in {
    val expectedTypes = Set(
      FileStatusService.Antivirus,
      FileStatusService.ChecksumMatch,
      FileStatusService.FFID,
      FileStatusService.Redaction,
      FileStatusService.Upload,
      FileStatusService.ServerChecksum,
      FileStatusService.ClientChecks,
      FileStatusService.ClosureMetadata,
      FileStatusService.DescriptiveMetadata
    )

    FileStatusService.allFileStatusTypes should equal(expectedTypes)
  }

  "'status types'" should "have the correct values assigned" in {
    FileStatusService.Antivirus should equal("Antivirus")
    FileStatusService.ChecksumMatch should equal("ChecksumMatch")
    FileStatusService.FFID should equal("FFID")
    FileStatusService.Redaction should equal("Redaction")
    FileStatusService.Upload should equal("Upload")
    FileStatusService.ServerChecksum should equal("ServerChecksum")
    FileStatusService.ClientChecks should equal("ClientChecks")
    FileStatusService.ClosureMetadata should equal("ClosureMetadata")
    FileStatusService.DescriptiveMetadata should equal("DescriptiveMetadata")
  }

  "'status values'" should "have the correct values assigned" in {
    FileStatusService.Success should equal("Success")
    FileStatusService.Mismatch should equal("Mismatch")
    FileStatusService.VirusDetected should equal("VirusDetected")
    FileStatusService.PasswordProtected should equal("PasswordProtected")
    FileStatusService.Zip should equal("Zip")
    FileStatusService.NonJudgmentFormat should equal("NonJudgmentFormat")
    FileStatusService.ZeroByteFile should equal("ZeroByteFile")
    FileStatusService.InProgress should equal("InProgress")
    FileStatusService.Completed should equal("Completed")
    FileStatusService.NotEntered should equal("NotEntered")
  }

  "'defaultStatuses'" should "contain the correct statuses and values" in {
    FileStatusService.defaultStatuses.size shouldBe 2
    FileStatusService.defaultStatuses(ClosureMetadata) should equal(NotEntered)
    FileStatusService.defaultStatuses(DescriptiveMetadata) should equal(NotEntered)
  }

  "getConsignmentFileProgress" should "return total processed files if all checks are successful" in {
    val rows = (1 to 5)
      .flatMap(_ => Seq(fileStatusRow(FFID, Success), fileStatusRow(ChecksumMatch, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(Set(FFID, ChecksumMatch, Antivirus), rows)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(5)
    result.checksumProgress.filesProcessed should equal(5)
    result.ffidProgress.filesProcessed should equal(5)
  }

  "getConsignmentFileProgress" should "return total processed files if some checks have failed" in {
    val successfulRows: Seq[nationalarchives.Tables.FilestatusRow] = (1 to 4)
      .flatMap(_ => Seq(fileStatusRow(FFID, Success), fileStatusRow(ChecksumMatch, Success), fileStatusRow(Antivirus, Success)))
    val failedRows = Seq(fileStatusRow(FFID, Failed), fileStatusRow(ChecksumMatch, Failed), fileStatusRow(Antivirus, Failed))

    mockResponse(Set(FFID, ChecksumMatch, Antivirus), successfulRows ++ failedRows)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(5)
    result.checksumProgress.filesProcessed should equal(5)
    result.ffidProgress.filesProcessed should equal(5)
  }

  "getConsignmentFileProgress" should "return zero processed files if there are no file status rows" in {
    mockResponse(Set(FFID, ChecksumMatch, Antivirus), Nil)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(0)
    result.checksumProgress.filesProcessed should equal(0)
    result.ffidProgress.filesProcessed should equal(0)
  }

  private def convertFileStatusRowToAddFileStatusInput(filestatusRows: List[FilestatusRow]): List[AddFileStatusInput] = {
    filestatusRows.map { filestatusRow =>
      AddFileStatusInput(filestatusRow.fileid, filestatusRow.statustype, filestatusRow.value)
    }
  }

  private class FileStatusServiceTestSetUp {
    val userId: UUID = UUID.randomUUID()
    val fileId1: UUID = UUID.randomUUID()
    val fileId2: UUID = UUID.randomUUID()

    val mockCustomMetadataService: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
    val mockFileStatusRepository: FileStatusRepository = mock[FileStatusRepository]
    val mockDisplayPropertiesService: DisplayPropertiesService = mock[DisplayPropertiesService]

    private val fixedUUIDSource = new FixedUUIDSource()
    fixedUUIDSource.reset
    val service = new FileStatusService(mockFileStatusRepository, mockCustomMetadataService, mockDisplayPropertiesService)

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

  def createFileStatusService(): FileStatusService =
    new FileStatusService(fileStatusRepositoryMock, customMetadataServiceMock, displayPropertiesServiceMock)
}
