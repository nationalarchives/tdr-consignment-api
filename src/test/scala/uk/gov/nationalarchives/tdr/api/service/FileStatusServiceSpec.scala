package uk.gov.nationalarchives.tdr.api.service

import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{FileRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput}
import uk.gov.nationalarchives.tdr.api.service
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

    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])

    val addFileStatusInput = AddFileStatusInput(UUID.randomUUID(), "Upload", "Success")
    val repositoryReturnValue = Future(Seq(FilestatusRow(UUID.randomUUID(), addFileStatusInput.fileId, "Upload", "Success", Timestamp.from(Instant.now()))))
    when(fileStatusRepositoryMock.addFileStatuses(fileStatusCaptor.capture())).thenReturn(repositoryReturnValue)

    val response = createFileStatusService().addFileStatuses(AddMultipleFileStatusesInput(addFileStatusInput :: Nil)).futureValue.head

    val fileStatusRowActual = fileStatusCaptor.getValue.head
    fileStatusRowActual.statustype should equal(addFileStatusInput.statusType)
    fileStatusRowActual.value should equal(addFileStatusInput.statusValue)
    fileStatusRowActual.fileid should equal(addFileStatusInput.fileId)
    fileStatusRowActual.createddatetime.before(Timestamp.from(Instant.now())) shouldBe true
    response.fileId should equal(addFileStatusInput.fileId)
    response.statusValue should equal(addFileStatusInput.statusValue)
    response.statusType should equal(addFileStatusInput.statusType)
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
      FileStatusService.ClientChecks
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
    FileStatusService.defaultStatuses.get(ClosureMetadata).get should equal(NotEntered)
    FileStatusService.defaultStatuses.get(DescriptiveMetadata).get should equal(NotEntered)
  }

  def createFileStatusService(): FileStatusService =
    new FileStatusService(fileStatusRepositoryMock, fixedUuidSource)
}
