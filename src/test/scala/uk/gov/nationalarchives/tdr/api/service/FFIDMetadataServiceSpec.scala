package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any

import java.sql.Timestamp
import java.util.UUID
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{ConsignmentRow, DisallowedpuidsRow, FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataInputMatches, FFIDMetadataInputValues, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val successStatusValue = "Success"

  "checkStatus" should "return the 'success' status for a valid puid value on 'standard' consignment type" in {
    val validPuid = "fmt/1"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(validPuid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(validPuid)

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(validPuid, consignmentType = "standard").futureValue
    status shouldBe successStatusValue
  }

  "checkStatus" should "return a non 'success' status for an active disallowed puid value on 'standard' consignment type" in {
    val passwordProtectedPuid = "fmt/494"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(passwordProtectedPuid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(passwordProtectedPuid, "DisallowedReason")

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(passwordProtectedPuid, "standard").futureValue
    status shouldBe "DisallowedReason"
  }

  "checkStatus" should "return a non 'success' status for an inactive disallowed puid on 'standard' consignment type" in {
    val inactiveDisallowedPuid = "x-fmt/409"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(inactiveDisallowedPuid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(inactiveDisallowedPuid, "NonActiveDisallowedReason", false)

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(inactiveDisallowedPuid, "standard").futureValue
    status shouldBe "NonActiveDisallowedReason"
  }

  "checkStatus" should "return the 'success' status for a valid judgment puid value on 'judgment' consignment type" in {
    val validJudgmentPuid = "fmt/412"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(validJudgmentPuid, existInAllowedPuid = true)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(validJudgmentPuid)

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(validJudgmentPuid, consignmentType = "judgment").futureValue
    status shouldBe successStatusValue
  }

  "checkStatus" should "return 'NonJudgmentFormat' status for a non-judgment inactive disallowed puid value on 'judgment' consignment type" in {
    val nonJudgmentPuid = "fmt/1"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(nonJudgmentPuid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(nonJudgmentPuid, "InactiveDisallowedReason", false)

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(nonJudgmentPuid, "judgment").futureValue
    status shouldBe "NonJudgmentFormat"
  }

  "checkStatus" should "return a non 'success' status for an active disallowed puid value on 'judgment' consignment type" in {
    val passwordProtectedPuid = "fmt/494"
    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(passwordProtectedPuid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(passwordProtectedPuid, "DisallowedReason")

    val service = new FFIDMetadataService(
      mock[FFIDMetadataRepository],
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val status = service.checkStatus(passwordProtectedPuid, "judgment").futureValue
    status shouldBe "DisallowedReason"
  }

  "addFFIDMetadata" should "call the repositories with the correct values" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUUIDSource = new FixedUUIDSource
    val fixedFileMetadataId = fixedUUIDSource.uuid
    val fixedConsignmentId = fixedUUIDSource.uuid
    val fixedUserId = fixedUUIDSource.uuid
    val fixedBodyId = fixedUUIDSource.uuid
    val puid = "puid"
    fixedUUIDSource.reset

    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]
    val fileRepository = mock[FileRepository]

    val metadataCaptor: ArgumentCaptor[List[FfidmetadataRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadataRow]])
    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])
    val fileIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val mockMetadataRow: List[FfidmetadataRow] = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now)) :: Nil
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.head.ffidmetadataid, puid)

    val mockMetadataResponse = Future(mockMetadataRow)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    val mockConsignmentRow = getMockConsignmentRow(fixedConsignmentId, fixedUserId, bodyId = fixedBodyId)
    val mockGetConsignmentResponse = Future(List(mockConsignmentRow))

    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(puid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(puid)

    when(fileRepository.getConsignmentForFile(fileIdCaptor.capture())).thenReturn(mockGetConsignmentResponse)
    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture(), fileStatusCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
      fileRepository,
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )
    service.addFFIDMetadata(getMetadataInput(fixedFileUuid)).futureValue
    fileIdCaptor.getValue shouldBe fixedFileUuid
    metadataCaptor.getValue should equal(mockMetadataRow)
  }

  "addFFIDMetadata" should "create ffid metadata given the correct arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUUIDSource = new FixedUUIDSource
    val fixedFileMetadataId = fixedUUIDSource.uuid
    val fixedConsignmentId = fixedUUIDSource.uuid
    val fixedUserId = fixedUUIDSource.uuid
    val fixedBodyId = fixedUUIDSource.uuid
    val puid = "puid"
    fixedUUIDSource.reset

    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]
    val fileRepository = mock[FileRepository]

    val metadataCaptor: ArgumentCaptor[List[FfidmetadataRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadataRow]])
    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])
    val fileIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val mockMetadataRow: FfidmetadataRow = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now))
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.ffidmetadataid, puid)

    val mockMetadataResponse = Future(mockMetadataRow :: Nil)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    val mockConsignmentRow = getMockConsignmentRow(fixedConsignmentId, fixedUserId, bodyId = fixedBodyId)
    val mockGetConsignmentResponse = Future(List(mockConsignmentRow))

    val allowedPuidsRepositoryMock = mockAllowedPuidResponse(puid)
    val disallowedPuidsRepositoryMock = mockDisallowedPuidResponse(puid)
    when(fileRepository.getConsignmentForFile(fileIdCaptor.capture())).thenReturn(mockGetConsignmentResponse)
    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture(), fileStatusCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
      fileRepository,
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )
    val result = service.addFFIDMetadata(getMetadataInput(fixedFileUuid)).futureValue.head
    result.fileId shouldEqual fixedFileUuid
    result.software shouldEqual "software"
    result.softwareVersion shouldEqual "softwareVersion"
    result.binarySignatureFileVersion shouldEqual "binaryVersion"
    result.containerSignatureFileVersion shouldEqual "containerVersion"
    result.method shouldEqual "method"
    result.datetime shouldEqual Timestamp.from(FixedTimeSource.now).getTime

    result.matches.size should equal(1)
    val matches = result.matches.head
    matches.extension.get shouldEqual "ext"
    matches.identificationBasis shouldEqual "identificationBasis"
    matches.puid.get shouldEqual "puid"
  }

  "addFFIDMetadata" should "throw an error if there are no ffid matches" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]
    val fileRepository = mock[FileRepository]
    val allowedPuidsRepositoryMock = mock[AllowedPuidsRepository]
    val disallowedPuidsRepositoryMock = mock[DisallowedPuidsRepository]
    val inputValues = FFIDMetadataInputValues(fixedFileUuid, "software", "softwareVersion", "binaryVersion", "containerVersion", "method", List())
    val inputWithNoMatches = FFIDMetadataInput(inputValues :: Nil)

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
      fileRepository,
      allowedPuidsRepositoryMock,
      disallowedPuidsRepositoryMock,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val thrownException = intercept[Exception] {
      service.addFFIDMetadata(inputWithNoMatches)
    }

    verify(metadataRepository, times(0)).addFFIDMetadata(any[FfidmetadataRow], any[List[FilestatusRow]])
    verify(matchesRepository, times(0)).addFFIDMetadataMatches(any[List[FfidmetadatamatchesRow]])

    thrownException.getMessage should include("No ffid matches for file 07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
  }

  "addFFIDMetadata" should "create metadata for multiple files" in {
    // TODO This
  }

  "getFFIDMetadata" should "call the repository with the correct arguments" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val selectedFileIdsCaptor: ArgumentCaptor[Option[Set[UUID]]] = ArgumentCaptor.forClass(classOf[Option[Set[UUID]]])
    val consignmentId = UUID.randomUUID()

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentIdCaptor.capture(), selectedFileIdsCaptor.capture())).thenReturn(Future(Seq()))

    val service = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      mock[AllowedPuidsRepository],
      mock[DisallowedPuidsRepository],
      FixedTimeSource,
      new FixedUUIDSource()
    )
    service.getFFIDMetadata(consignmentId)

    consignmentIdCaptor.getValue should equal(consignmentId)
    selectedFileIdsCaptor.getValue should equal(None)
  }

  "getFFIDMetadata" should "return multiple rows and matches for multiple files" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val consignmentId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val metadataRowOne = FfidmetadataRow(
      UUID.randomUUID(),
      fileIdOne,
      "softwareRow1",
      "softwareVersionRow1",
      Timestamp.from(FixedTimeSource.now),
      "binarySignatureFileVersionRow1",
      "containerSignatureFileVersionRow1",
      "methodRow1"
    )
    val matchesRowOneMatchOne = FfidmetadatamatchesRow(UUID.randomUUID(), Option("extensionRow1Match1"), "basisRow1Match1", Option("puidRow1Match1"))
    val matchesRowOneMatchTwo = FfidmetadatamatchesRow(UUID.randomUUID(), Option("extensionRow1Match2"), "basisRow1Match2", Option("puidRow1Match2"))

    val metadataRowTwo = FfidmetadataRow(
      UUID.randomUUID(),
      fileIdTwo,
      "softwareRow2",
      "softwareVersionRow2",
      Timestamp.from(FixedTimeSource.now),
      "binarySignatureFileVersionRow2",
      "containerSignatureFileVersionRow2",
      "methodRow2"
    )
    val matchesRowTwoMatchOne = FfidmetadatamatchesRow(UUID.randomUUID(), Option("extensionRow2Match1"), "basisRow2Match1", Option("puidRow2Match1"))

    when(ffidMetadataRepositoryMock.getFFIDMetadata(any[UUID], any[Option[Set[UUID]]])).thenReturn(
      Future(
        Seq(
          (metadataRowOne, matchesRowOneMatchOne),
          (metadataRowOne, matchesRowOneMatchTwo),
          (metadataRowTwo, matchesRowTwoMatchOne)
        )
      )
    )

    val service = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      mock[FileRepository],
      mock[AllowedPuidsRepository],
      mock[DisallowedPuidsRepository],
      FixedTimeSource,
      new FixedUUIDSource()
    )
    val response = service.getFFIDMetadata(consignmentId).futureValue

    val ffidMetadataRowOne = response.find(_.fileId == fileIdOne).get
    val ffidMetadataRowTwo = response.find(_.fileId == fileIdTwo).get
    checkMetadataRow(ffidMetadataRowOne, 1)
    checkMetadataRow(ffidMetadataRowTwo, 2)

    checkMatch(ffidMetadataRowOne.matches.head, 1, 1)
    checkMatch(ffidMetadataRowOne.matches.last, 1, 2)
    checkMatch(ffidMetadataRowTwo.matches.head, 2, 1)
  }

  def checkMetadataRow(ffidMetadata: FFIDMetadata, rowNumber: Int): Assertion = {
    ffidMetadata.binarySignatureFileVersion should equal(s"binarySignatureFileVersionRow$rowNumber")
    ffidMetadata.containerSignatureFileVersion should equal(s"containerSignatureFileVersionRow$rowNumber")
    ffidMetadata.method should equal(s"methodRow$rowNumber")
    ffidMetadata.software should equal(s"softwareRow$rowNumber")
    ffidMetadata.softwareVersion should equal(s"softwareVersionRow$rowNumber")
  }

  def checkMatch(ffidMetadataMatch: FFIDMetadataMatches, rowNumber: Int, matchNumber: Int): Assertion = {
    ffidMetadataMatch.extension.get should equal(s"extensionRow${rowNumber}Match$matchNumber")
    ffidMetadataMatch.identificationBasis should equal(s"basisRow${rowNumber}Match$matchNumber")
    ffidMetadataMatch.puid.get should equal(s"puidRow${rowNumber}Match$matchNumber")
  }

  private def getMetadataInput(fixedFileUuid: UUID): FFIDMetadataInput = {
    val inputValues = FFIDMetadataInputValues(
      fixedFileUuid,
      "software",
      "softwareVersion",
      "binaryVersion",
      "containerVersion",
      "method",
      List(FFIDMetadataInputMatches(Some("ext"), "identificationBasis", Some("puid")))
    )
    FFIDMetadataInput(inputValues :: Nil)
  }

  private def getMatchesRow(fileMetadataId: UUID, puid: String = "puid") = {
    FfidmetadatamatchesRow(fileMetadataId, Some("ext"), "identificationBasis", Some(puid))
  }

  private def getMockMetadataRow(ffidMetadataId: UUID, fixedFileUuid: UUID, dummyTimestamp: Timestamp): FfidmetadataRow = {
    FfidmetadataRow(ffidMetadataId, fixedFileUuid, "software", "softwareVersion", dummyTimestamp, "binaryVersion", "containerVersion", "method")
  }

  private def getMockConsignmentRow(consignmentId: UUID, userId: UUID, consignmentType: String = "standard", bodyId: UUID): ConsignmentRow = {
    ConsignmentRow(
      consignmentId,
      userid = userId,
      datetime = Timestamp.from(FixedTimeSource.now),
      consignmentsequence = 1L,
      consignmentreference = "consignmentRef",
      consignmenttype = consignmentType,
      bodyid = bodyId
    )
  }

  def mockAllowedPuidResponse(puid: String, existInAllowedPuid: Boolean = false): AllowedPuidsRepository = {
    val allowedPuidsRepository: AllowedPuidsRepository = mock[AllowedPuidsRepository]
    val mockAllowedPuidsResponse = Future(existInAllowedPuid)
    when(allowedPuidsRepository.checkAllowedPuidExists(puid)).thenReturn(mockAllowedPuidsResponse)
    allowedPuidsRepository
  }

  def mockDisallowedPuidResponse(puid: String, disallowedReason: String = "Success", active: Boolean = true): DisallowedPuidsRepository = {
    val disallowedPuidsRepository: DisallowedPuidsRepository = mock[DisallowedPuidsRepository]
    val mockDisallowedPuidsResponse = DisallowedpuidsRow(puid, s"description for $puid", Timestamp.from(FixedTimeSource.now), None, disallowedReason, active)
    when(disallowedPuidsRepository.getDisallowedPuid(puid)).thenReturn(Future(Some(mockDisallowedPuidsResponse)))
    disallowedPuidsRepository
  }
}
