package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.mockito.stubbing.ScalaOngoingStubbing
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{DisallowedPuidsRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with BeforeAndAfterEach {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val disallowedPuidsRepositoryMock: DisallowedPuidsRepository = mock[DisallowedPuidsRepository]
  val consignmentId: UUID = UUID.randomUUID()

  val activeDisallowedPuid1 = "ActiveDisallowedPuid1"
  val activeDisallowedPuid2 = "ActiveDisallowedPuid2"
  val inactiveDisallowedPuid = "InactiveDisallowedPuid"
  val activeDisallowedPuidsResponse: Seq[String] = Seq(activeDisallowedPuid1, activeDisallowedPuid2)

  when(disallowedPuidsRepositoryMock.activeReasons()).thenReturn(Future(activeDisallowedPuidsResponse))

  override def beforeEach(): Unit = {
    reset(fileStatusRepositoryMock)
  }

  def mockResponse(statusType: String, rows: Seq[FilestatusRow]): ScalaOngoingStubbing[Future[Seq[FilestatusRow]]] =
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, statusType)).thenReturn(Future(rows))

  def fileStatusRow(statusType: String, value: String): FilestatusRow =
    FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), statusType, value, Timestamp.from(Instant.now))

  "allChecksSucceeded" should "return true if the checksum match, antivirus and ffid statuses are 'Success'" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return true if the ffid status is an inactive disallowed status and the checksum match and antivirus statuses are 'Success'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, inactiveDisallowedPuid)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum match status is 'Mismatch' and the antivirus and ffid statuses are 'Success'" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Mismatch)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected' and the checksum and ffid statuses are 'Success'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the ffid status is an active disallowed status and the checksum match and antivirus statuses are 'Success'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, activeDisallowedPuid1)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if antivirus status is 'VirusDetected', " +
    "the checksum match status is 'Mismatch' and the ffid status an active disallowed puid" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Mismatch)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, activeDisallowedPuid1)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if antivirus status is 'VirusDetected', " +
    "the checksum match status is 'Mismatch' and the ffid status is an inactive disallowed puid" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Mismatch)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, inactiveDisallowedPuid)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows and the checksum match and ffid statuses are 'Success'" in {
    mockResponse(Antivirus, Seq())
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum match file status rows and the antivirus and ffid statuses are 'Success" in {
    mockResponse(ChecksumMatch, Seq())
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no ffid file status rows and the antivirus and checksum match statuses are 'Success" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq())
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure " +
    "and multiple successful antivirus and ffid rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum match rows including a failure, " +
    "ffid success and inactive disallowed puid rows, and multiple successful antivirus rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Mismatch), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, inactiveDisallowedPuid), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure " +
    "and multiple successful checksum match and ffid rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows including a failure, " +
    "ffid success and inactive disallowed puid rows, and multiple successful checksum matches" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, inactiveDisallowedPuid), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple ffid rows including an active disallowed puid " +
    "and multiple successful checksum match and antivirus rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, activeDisallowedPuid1), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return true if there are multiple ffid rows including success and inactive disallowed puid " +
    "and multiple successful checksum match and antivirus rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, inactiveDisallowedPuid), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if there are multiple ffid failure rows and multiple successful checksum match and antivirus rows" in {
    mockResponse(ChecksumMatch, Seq(fileStatusRow(ChecksumMatch, Success), fileStatusRow(ChecksumMatch, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, activeDisallowedPuid1), fileStatusRow(FFID, activeDisallowedPuid2)))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "getFileStatus" should "return a Map Consisting of a FileId key and status value" in {
    mockResponse(FFID, Seq(FilestatusRow(UUID.randomUUID(), consignmentId, FFID, Success, Timestamp.from(Instant.now))))
    val response = new FileStatusService(fileStatusRepositoryMock, disallowedPuidsRepositoryMock).getFileStatus(consignmentId).futureValue
    val expected = Map(consignmentId -> Success)
    response should equal(expected)
  }

  "'status types'" should "have the correct values assigned" in {
    FileStatusService.Antivirus should equal("Antivirus")
    FileStatusService.ChecksumMatch should equal("ChecksumMatch")
    FileStatusService.FFID should equal("FFID")
  }

  "'status value'" should "have the correct values assigned" in {
    FileStatusService.Mismatch should equal("Mismatch")
    FileStatusService.NonJudgmentFormat should equal("NonJudgmentFormat")
    FileStatusService.PasswordProtected should equal("PasswordProtected")
    FileStatusService.VirusDetected should equal("VirusDetected")
    FileStatusService.ZeroByteFile should equal("ZeroByteFile")
    FileStatusService.Zip should equal("Zip")
  }
}
