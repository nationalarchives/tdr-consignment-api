package uk.gov.nationalarchives.tdr.api.service

import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{FileRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput}
import uk.gov.nationalarchives.tdr.api.utils.Statuses._
import uk.gov.nationalarchives.tdr.api.utils.{FixedUUIDSource, Statuses}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with BeforeAndAfterEach {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fileRepositoryMock: FileRepository = mock[FileRepository]
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

  "addFileStatuses" should "add a file status row in the db" in {

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

  "allChecksSucceeded" should "return true if the checksum match, antivirus, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum match status is 'Mismatch' and the antivirus, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(ChecksumMatchType.id, MismatchValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected' and the checksum, ffid and redaction statuses are 'Success'" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(AntivirusType.id, VirusDetectedValue.value),
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows, the checksum match and ffid statuses are 'Success' and the redacted status is success" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum match file status rows, the antivirus and ffid statuses are 'Success' and the redaction status is success" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(fileStatusRow(AntivirusType.id, SuccessValue.value), fileStatusRow(FFIDType.id, SuccessValue.value))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no ffid file status rows, the antivirus and checksum match statuses are 'Success' and the redaction status is success" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure " +
    "and multiple successful antivirus, ffid and redaction rows" in {
      mockResponse(
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
        Seq(
          fileStatusRow(ChecksumMatchType.id, MismatchValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure, " +
    "ffid success and multiple successful antivirus and redaction rows" in {
      mockResponse(
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
        Seq(
          fileStatusRow(ChecksumMatchType.id, MismatchValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure " +
    "and multiple successful checksum match, ffid and redaction rows" in {
      mockResponse(
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
        Seq(
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, VirusDetectedValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure, " +
    "ffid success and multiple successful checksum and redaction matches" in {
      mockResponse(
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
        Seq(
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, VirusDetectedValue.value),
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value),
          fileStatusRow(RedactionType.id, SuccessValue.value)
        )
      )
      val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
      response should equal(false)
    }

  "allChecksSucceeded" should "return true if there are multiple ffid success rows and multiple successful checksum match, antivirus and redaction rows" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, SuccessValue.value)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if there are missing original files with a redacted file" in {
    mockResponse(
      Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id),
      Seq(
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(AntivirusType.id, SuccessValue.value),
        fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
        fileStatusRow(FFIDType.id, SuccessValue.value),
        fileStatusRow(RedactionType.id, "MissingOriginalFile")
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "getFileStatuses" should "return expected file status types" in {
    val fileId1 = UUID.randomUUID()
    val fileId2 = UUID.randomUUID()

    mockResponse(
      Set(FFIDType.id, UploadType.id, AntivirusType.id),
      Seq(
        FilestatusRow(UUID.randomUUID(), fileId1, FFIDType.id, SuccessValue.value, Timestamp.from(Instant.now)),
        FilestatusRow(UUID.randomUUID(), fileId2, UploadType.id, SuccessValue.value, Timestamp.from(Instant.now)),
        FilestatusRow(UUID.randomUUID(), fileId1, AntivirusType.id, VirusDetectedValue.value, Timestamp.from(Instant.now))
      )
    )

    val response = createFileStatusService().getFileStatuses(consignmentId, Set(FFIDType.id, UploadType.id, AntivirusType.id)).futureValue
    response.size shouldBe 3
    val statusFFID = response.find(_.statusType == FFIDType.id).get
    statusFFID.fileId should equal(fileId1)
    statusFFID.statusType should equal(FFIDType.id)
    statusFFID.statusValue should equal(SuccessValue.value)

    val statusUpload = response.find(_.statusType == UploadType.id).get
    statusUpload.fileId should equal(fileId2)
    statusUpload.statusType should equal(UploadType.id)
    statusUpload.statusValue should equal(SuccessValue.value)

    val statusAntivirus = response.find(_.statusType == AntivirusType.id).get
    statusAntivirus.fileId should equal(fileId1)
    statusAntivirus.statusType should equal(AntivirusType.id)
    statusAntivirus.statusValue should equal(VirusDetectedValue.value)
  }

  "getFileStatuses" should "return empty status list if no statuses present" in {
    mockResponse(
      Set(FFIDType.id, UploadType.id, AntivirusType.id),
      Seq()
    )

    val response = createFileStatusService().getFileStatuses(consignmentId, Set(FFIDType.id, UploadType.id, AntivirusType.id)).futureValue
    response.size shouldBe 0
  }

  "allFileStatusTypes" should "include all file status types" in {
    val expectedTypes = Set(
      Statuses.AntivirusType.id,
      Statuses.ChecksumMatchType.id,
      Statuses.FFIDType.id,
      Statuses.RedactionType.id,
      Statuses.UploadType.id,
      Statuses.ServerChecksumType.id,
      Statuses.ClientChecksType.id
    )

    FileStatusService.allFileStatusTypes should equal(expectedTypes)
  }

  "getConsignmentFileProgress" should "return total processed files if all checks are successful" in {
    val rows = (1 to 5)
      .flatMap(_ =>
        Seq(
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value)
        )
      )
    mockResponse(Set(FFIDType.id, ChecksumMatchType.id, AntivirusType.id), rows)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(5)
    result.checksumProgress.filesProcessed should equal(5)
    result.ffidProgress.filesProcessed should equal(5)
  }

  "getConsignmentFileProgress" should "return total processed files if some checks have failed" in {
    val successfulRows: Seq[nationalarchives.Tables.FilestatusRow] = (1 to 4)
      .flatMap(_ =>
        Seq(
          fileStatusRow(FFIDType.id, SuccessValue.value),
          fileStatusRow(ChecksumMatchType.id, SuccessValue.value),
          fileStatusRow(AntivirusType.id, SuccessValue.value)
        )
      )
    val failedRows = Seq(
      fileStatusRow(FFIDType.id, FailedValue.value),
      fileStatusRow(ChecksumMatchType.id, FailedValue.value),
      fileStatusRow(AntivirusType.id, FailedValue.value)
    )

    mockResponse(Set(FFIDType.id, ChecksumMatchType.id, AntivirusType.id), successfulRows ++ failedRows)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(5)
    result.checksumProgress.filesProcessed should equal(5)
    result.ffidProgress.filesProcessed should equal(5)
  }

  "getConsignmentFileProgress" should "return zero processed files if there are no file status rows" in {
    mockResponse(Set(FFIDType.id, ChecksumMatchType.id, AntivirusType.id), Nil)

    val service = createFileStatusService()
    val result = service.getConsignmentFileProgress(consignmentId).futureValue

    result.antivirusProgress.filesProcessed should equal(0)
    result.checksumProgress.filesProcessed should equal(0)
    result.ffidProgress.filesProcessed should equal(0)
  }

  def createFileStatusService(): FileStatusService =
    new FileStatusService(fileStatusRepositoryMock)
}
