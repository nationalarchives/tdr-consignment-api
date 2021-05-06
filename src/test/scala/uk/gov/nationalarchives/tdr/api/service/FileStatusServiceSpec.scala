package uk.gov.nationalarchives.tdr.api.service

import org.mockito.MockitoSugar
import org.mockito.stubbing.ScalaOngoingStubbing
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Checksum, Mismatch, Success, VirusDetected}

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

  "allChecksSucceeded" should "return true if the checksum status is Success and antivirus status is Success" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(true)
  }

  "allChecksSucceeded" should "return false if the checksum status is Mismatch and antivirus status is Success" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is VirusDetected and the checksum status is Success" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if the antivirus status is VirusDetected and the checksum status is Mismatch" in {
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, VirusDetected)))
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no antivirus file status rows and the checksum status is success" in {
    mockResponse(Antivirus, Seq())
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are no checksum file status rows and the antivirus status is success" in {
    mockResponse(Checksum, Seq())
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple checksum rows with one failure and multiple successful antivirus rows" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Mismatch), fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, Success)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }

  "allChecksSucceeded" should "return false if there are multiple antivirus rows with one failure and multiple successful checksum rows" in {
    mockResponse(Checksum, Seq(fileStatusRow(Checksum, Success), fileStatusRow(Checksum, Success)))
    mockResponse(Antivirus, Seq(fileStatusRow(Antivirus, Success), fileStatusRow(Antivirus, VirusDetected)))
    val response = new FileStatusService(fileStatusRepositoryMock).allChecksSucceeded(consignmentId).futureValue
    response should equal(false)
  }
}
