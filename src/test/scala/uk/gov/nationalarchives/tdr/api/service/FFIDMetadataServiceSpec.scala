package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{ConsignmentRow, FfidmetadataRow, FfidmetadatamatchesRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataInputMatches, FFIDMetadataInputValues, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val successStatusValue = "Success"

  "addFFIDMetadata" should "call the repositories with the correct values" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUUIDSource = new FixedUUIDSource
    val fixedFileMetadataId = fixedUUIDSource.uuid
    val puid = "puid"
    fixedUUIDSource.reset

    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]

    val metadataCaptor: ArgumentCaptor[List[FfidmetadataRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadataRow]])
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])

    val mockMetadataRow: FfidmetadataRow = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now))
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.ffidmetadataid, puid)

    val mockMetadataResponse = Future(mockMetadataRow :: Nil)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
      FixedTimeSource,
      new FixedUUIDSource()
    )
    service.addFFIDMetadata(getMetadataInput(fixedFileUuid)).futureValue
    metadataCaptor.getValue.head should equal(mockMetadataRow)
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
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])
    val fileIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val mockMetadataRow: FfidmetadataRow = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now))
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.ffidmetadataid, puid)

    val mockMetadataResponse = Future(mockMetadataRow :: Nil)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    val mockConsignmentRow = getMockConsignmentRow(fixedConsignmentId, fixedUserId, bodyId = fixedBodyId)
    val mockGetConsignmentResponse = Future(List(mockConsignmentRow))

    when(fileRepository.getConsignmentForFile(fileIdCaptor.capture())).thenReturn(mockGetConsignmentResponse)
    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
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

    val inputWithNoMatches = FFIDMetadataInputValues(
      fixedFileUuid,
      "software",
      "softwareVersion",
      "binaryVersion",
      "containerVersion",
      "method",
      List()
    )

    val service = new FFIDMetadataService(
      metadataRepository,
      matchesRepository,
      FixedTimeSource,
      new FixedUUIDSource()
    )

    val thrownException = intercept[Exception] {
      service.addFFIDMetadata(FFIDMetadataInput(inputWithNoMatches :: Nil))
    }

    verify(metadataRepository, times(0)).addFFIDMetadata(any[List[FfidmetadataRow]])
    verify(matchesRepository, times(0)).addFFIDMetadataMatches(any[List[FfidmetadatamatchesRow]])

    thrownException.getMessage should include("No ffid matches for file 07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
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

  private def getMetadataInput(fixedFileUuid: UUID) = {
    FFIDMetadataInput(
      FFIDMetadataInputValues(
        fixedFileUuid,
        "software",
        "softwareVersion",
        "binaryVersion",
        "containerVersion",
        "method",
        List(FFIDMetadataInputMatches(Some("ext"), "identificationBasis", Some("puid")))
      ) :: Nil
    )
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
}
