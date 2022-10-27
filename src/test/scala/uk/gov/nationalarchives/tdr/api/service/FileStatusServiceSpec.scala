package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.RedactedFiles
import uk.gov.nationalarchives.tdr.api.db.repository.{DisallowedPuidsRepository, FileRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.FixedUUIDSource

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with BeforeAndAfterEach {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val disallowedPuidsRepositoryMock: DisallowedPuidsRepository = mock[DisallowedPuidsRepository]
  val consignmentId: UUID = UUID.randomUUID()
  val fixedUuidSource = new FixedUUIDSource()

  val activeDisallowedPuid1 = "ActiveDisallowedPuid1"
  val activeDisallowedPuid2 = "ActiveDisallowedPuid2"
  val inactiveDisallowedPuid = "InactiveDisallowedPuid"
  val activeDisallowedPuidsResponse: Seq[String] = Seq(activeDisallowedPuid1, activeDisallowedPuid2)

  when(disallowedPuidsRepositoryMock.activeReasons()).thenReturn(Future(activeDisallowedPuidsResponse))

  override def beforeEach(): Unit = {
    reset(fileStatusRepositoryMock)
  }

  def mockResponse(statusTypes: Set[String], rows: Seq[FilestatusRow]): ScalaOngoingStubbing[Future[Seq[FilestatusRow]]] =
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, statusTypes)).thenReturn(Future(rows))

  def mockRedactedResponse(redactedResponse: Seq[RedactedFiles]): ScalaOngoingStubbing[Future[Seq[RedactedFiles]]] =
    when(fileRepositoryMock.getRedactedFilePairs(consignmentId, onlyNullValues = true)).thenReturn(Future(redactedResponse))

  def fileStatusRow(statusType: String, value: String): FilestatusRow =
    FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), statusType, value, Timestamp.from(Instant.now))


  "addFileStatus" should "add a file status row in the db" in {

    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])

    when(fileStatusRepositoryMock.addFileStatuses(fileStatusCaptor.capture())).thenReturn(Future(Seq()))

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Upload", "Success")
    val response = createFileStatusService().addFileStatus(addFileStatusInput).futureValue

    val fileStatusRowActual = fileStatusCaptor.getValue.head
    fileStatusRowActual.statustype should equal(addFileStatusInput.statusType)
    fileStatusRowActual.value should equal(addFileStatusInput.statusValue)
    fileStatusRowActual.fileid should equal(addFileStatusInput.fileId)
    fileStatusRowActual.createddatetime.before(Timestamp.from(Instant.now())) shouldBe true
    response.fileId should equal(addFileStatusInput.fileId)
    response.statusValue should equal(addFileStatusInput.statusValue)
    response.statusType should equal(addFileStatusInput.statusType)
  }

  "addFileStatus" should "not add a file status row if status type is invalid" in {

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Download", "Success")

    val thrownException = intercept[Exception] {
      createFileStatusService().addFileStatus(addFileStatusInput).futureValue
    }

    verify(fileStatusRepositoryMock, times(0)).addFileStatuses(any())
    thrownException.getMessage should include("Invalid FileStatus input: either 'Download' or 'Success'")
  }

  "addFileStatus" should "not add a file status row if status value is invalid" in {

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Upload", "Passed")

    val thrownException = intercept[Exception] {
      createFileStatusService().addFileStatus(addFileStatusInput).futureValue
    }

    verify(fileStatusRepositoryMock, times(0)).addFileStatuses(any())
    thrownException.getMessage should include("Invalid FileStatus input: either 'Upload' or 'Passed'")
  }

  "addFileStatus" should "not add a file status row if status type and status value are invalid" in {

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Download", "Passed")

    val thrownException = intercept[Exception] {
      createFileStatusService().addFileStatus(addFileStatusInput).futureValue
    }

    verify(fileStatusRepositoryMock, times(0)).addFileStatuses(any())
    thrownException.getMessage should include("Invalid FileStatus input: either 'Download' or 'Passed'")
  }

  "allChecksSucceeded" should "return true if the checksum match, antivirus and ffid statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return true if the ffid status is an inactive disallowed status and the checksum match and antivirus statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, inactiveDisallowedPuid))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum match status is 'Mismatch' and the antivirus and ffid statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected' and the checksum and ffid statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, VirusDetected), fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the ffid status is an active disallowed status and the checksum match and antivirus statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, activeDisallowedPuid1))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if antivirus status is 'VirusDetected', " +
    "the checksum match status is 'Mismatch' and the ffid status an active disallowed puid" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, VirusDetected), fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(FFID, activeDisallowedPuid1))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if antivirus status is 'VirusDetected', " +
    "the checksum match status is 'Mismatch' and the ffid status is an inactive disallowed puid" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, VirusDetected), fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(FFID, inactiveDisallowedPuid))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows and the checksum match and ffid statuses are 'Success'" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum match file status rows and the antivirus and ffid statuses are 'Success" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(FFID, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no ffid file status rows and the antivirus and checksum match statuses are 'Success" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success))
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure " +
    "and multiple successful antivirus and ffid rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Mismatch),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, Success),
        fileStatusRow(FFID, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure, " +
    "ffid success and inactive disallowed puid rows, and multiple successful antivirus rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Mismatch),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, inactiveDisallowedPuid),
        fileStatusRow(FFID, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure " +
    "and multiple successful checksum match and ffid rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
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
    "ffid success and inactive disallowed puid rows, and multiple successful checksum matches" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, VirusDetected),
        fileStatusRow(FFID, inactiveDisallowedPuid),
        fileStatusRow(FFID, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple ffid rows including an active disallowed puid " +
    "and multiple successful checksum match and antivirus rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, activeDisallowedPuid1),
        fileStatusRow(FFID, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return true if there are multiple ffid rows including success and inactive disallowed puid " +
    "and multiple successful checksum match and antivirus rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, inactiveDisallowedPuid),
        fileStatusRow(FFID, Success)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if there are multiple ffid failure rows and multiple successful checksum match and antivirus rows" in {
    mockRedactedResponse(Seq())
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(FFID, activeDisallowedPuid1),
        fileStatusRow(FFID, activeDisallowedPuid2)
      )
    )
    val response = createFileStatusService().allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are missing original files with a redacted file" in {
    mockRedactedResponse(Seq(RedactedFiles(UUID.randomUUID(), "redacted_R.txt", None, None)))
    mockResponse(
      Set(ChecksumMatch, Antivirus, FFID),
      Seq(
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(Antivirus, Success),
        fileStatusRow(ChecksumMatch, Success),
        fileStatusRow(FFID, Success)
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

  "'status types'" should "have the correct values assigned" in {
    FileStatusService.Antivirus should equal("Antivirus")
    FileStatusService.ChecksumMatch should equal("ChecksumMatch")
    FileStatusService.FFID should equal("FFID")
  }

  "'status values'" should "have the correct values assigned" in {
    FileStatusService.Mismatch should equal("Mismatch")
    FileStatusService.NonJudgmentFormat should equal("NonJudgmentFormat")
    FileStatusService.PasswordProtected should equal("PasswordProtected")
    FileStatusService.VirusDetected should equal("VirusDetected")
    FileStatusService.ZeroByteFile should equal("ZeroByteFile")
    FileStatusService.Zip should equal("Zip")
  }

  def createFileStatusService(): FileStatusService =
    new FileStatusService(fileRepositoryMock, fileStatusRepositoryMock, disallowedPuidsRepositoryMock, fixedUuidSource)
}
