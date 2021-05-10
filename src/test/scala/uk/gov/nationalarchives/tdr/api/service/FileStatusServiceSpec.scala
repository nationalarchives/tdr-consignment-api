package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.mockito.stubbing.ScalaOngoingStubbing
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Checksum, FFID, Mismatch, PasswordProtected, Success, VirusDetected}
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

class FileStatusServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with BeforeAndAfterEach {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val consignmentId: UUID = UUID.randomUUID()

  override def beforeEach(): Unit = {
    reset(fileStatusRepositoryMock)
  }

  def mockResponse(statusType: String, rows: Seq[FilestatusRow]): ScalaOngoingStubbing[Future[Seq[FilestatusRow]]] =
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, statusType)).thenReturn(Future(rows))

  def fileStatusRow(statusType: String, value: String): FilestatusRow =
    FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), statusType, value, Timestamp.from(Instant.now))

  "allChecksSucceeded" should "return true if the checksum, antivirus and ffid statuses are 'Success'" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum status is 'Mismatch' and the antivirus and ffid statuses are 'Success'" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected' and the checksum and ffid statuses are 'Success'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the ffid status is 'PasswordProtected' and the checksum and antivirus statuses are 'Success'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, PasswordProtected)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is 'VirusDetected', the checksum status is 'Mismatch' and the ffid status is 'PasswordProtected'" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, PasswordProtected)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows and the checksum and ffid statuses are 'Success'" in {
    mockResponse(Antivirus, Seq())
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum file status rows and the antivirus and ffid statuses are 'Success" in {
    mockResponse(Checksum, Seq())
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no ffid file status rows and the antivirus and checksum statuses are 'Success" in {
    mockResponse(Checksum, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq())
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum rows with one failure and multiple successful antivirus and ffid rows" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch), fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows with one failure and multiple successful checksum and ffid rows" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success), fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, Success), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple ffid rows with one failure and multiple successful checksum and antivirus rows" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success), fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    mockResponse(FFID, Seq(fileStatusRow(FFID, PasswordProtected), fileStatusRow(FFID, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }
}
