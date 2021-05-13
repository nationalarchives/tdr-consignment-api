package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any

import java.sql.Timestamp
import java.util.UUID
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FFIDMetadataMatchesRepository, FFIDMetadataRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataInputMatches, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "passwordProtectedPuids list" should "match the expected list of pronom ids for password protected files" in {
    val expectedPuids = List("fmt/494", "fmt/754", "fmt/755")

    val service = new FFIDMetadataService(mock[FFIDMetadataRepository], mock[FFIDMetadataMatchesRepository], FixedTimeSource, new FixedUUIDSource())
    service.passwordProtectedPuids should equal(expectedPuids)
  }

  "zipPuids list" should "matched the expected list of pronom ids for zip files" in {
    val expectedPuids = List("fmt/289", "fmt/329", "fmt/484", "fmt/508", "fmt/600", "fmt/610", "fmt/613", "fmt/614", "fmt/625",
      "fmt/626", "fmt/639", "fmt/656", "fmt/726", "fmt/842", "fmt/843", "fmt/844", "fmt/850", "fmt/866", "fmt/887", "fmt/1070",
      "fmt/1071", "fmt/1087", "fmt/1095", "fmt/1096", "fmt/1097", "fmt/1098", "fmt/1100", "fmt/1102", "fmt/1105", "fmt/1190",
      "fmt/1242", "fmt/1243", "fmt/1244", "fmt/1245", "fmt/1251", "fmt/1252", "fmt/1281", "fmt/1340", "fmt/1341", "fmt/1355",
      "fmt/1361", "fmt/1399", "x-fmt/157", "x-fmt/219", "x-fmt/263", "x-fmt/265", "x-fmt/266", "x-fmt/267", "x-fmt/268", "x-fmt/269",
      "x-fmt/412", "x-fmt/416", "x-fmt/429")

    val service = new FFIDMetadataService(mock[FFIDMetadataRepository], mock[FFIDMetadataMatchesRepository], FixedTimeSource, new FixedUUIDSource())
    service.zipPuids should equal(expectedPuids)
  }

  "addFFIDMetadata" should "call the repositories with the correct values" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUUIDSource = new FixedUUIDSource
    val fixedFileMetadataId = fixedUUIDSource.uuid
    fixedUUIDSource.reset

    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]

    val metadataCaptor: ArgumentCaptor[FfidmetadataRow] = ArgumentCaptor.forClass(classOf[FfidmetadataRow])
    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] = ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])

    val mockMetadataRow: FfidmetadataRow = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now))
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.ffidmetadataid)

    val mockMetadataResponse = Future(mockMetadataRow)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture(), fileStatusCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(metadataRepository, matchesRepository, FixedTimeSource, new FixedUUIDSource())
    service.addFFIDMetadata(getMetadataInput(fixedFileUuid))
    metadataCaptor.getValue should equal(mockMetadataRow)
  }

  "addFFIDMetadata" should "create ffid metadata given the correct arguments" in {
    val fixedFileUuid = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
    val fixedUUIDSource = new FixedUUIDSource
    val fixedFileMetadataId = fixedUUIDSource.uuid
    fixedUUIDSource.reset

    val metadataRepository = mock[FFIDMetadataRepository]
    val matchesRepository = mock[FFIDMetadataMatchesRepository]

    val metadataCaptor: ArgumentCaptor[FfidmetadataRow] =  ArgumentCaptor.forClass(classOf[FfidmetadataRow])
    val fileStatusCaptor: ArgumentCaptor[List[FilestatusRow]] = ArgumentCaptor.forClass(classOf[List[FilestatusRow]])
    val matchCaptor: ArgumentCaptor[List[FfidmetadatamatchesRow]] =  ArgumentCaptor.forClass(classOf[List[FfidmetadatamatchesRow]])

    val mockMetadataRow: FfidmetadataRow = getMockMetadataRow(fixedFileMetadataId, fixedFileUuid, Timestamp.from(FixedTimeSource.now))
    val mockMetadataMatchesRow = getMatchesRow(mockMetadataRow.ffidmetadataid)

    val mockMetadataResponse = Future(mockMetadataRow)
    val mockMetadataMatchesResponse = Future(List(mockMetadataMatchesRow))

    when(metadataRepository.addFFIDMetadata(metadataCaptor.capture(), fileStatusCaptor.capture())).thenReturn(mockMetadataResponse)
    when(matchesRepository.addFFIDMetadataMatches(matchCaptor.capture())).thenReturn(mockMetadataMatchesResponse)

    val service = new FFIDMetadataService(metadataRepository, matchesRepository, FixedTimeSource, new FixedUUIDSource())
    val result = service.addFFIDMetadata(getMetadataInput(fixedFileUuid)).futureValue
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

  "getFFIDMetadata" should "call the repository with the correct arguments" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val consignmentId = UUID.randomUUID()

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentIdCaptor.capture())).thenReturn(Future(Seq()))

    val service = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, new FixedUUIDSource())
    service.getFFIDMetadata(consignmentId)

    consignmentIdCaptor.getValue should equal(consignmentId)
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

    when(ffidMetadataRepositoryMock.getFFIDMetadata(any[UUID])).thenReturn(
      Future(Seq(
        (metadataRowOne,matchesRowOneMatchOne ), (metadataRowOne, matchesRowOneMatchTwo), (metadataRowTwo, matchesRowTwoMatchOne)
      ))
    )

    val service = new FFIDMetadataService(ffidMetadataRepositoryMock, mock[FFIDMetadataMatchesRepository], FixedTimeSource, new FixedUUIDSource())
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
      fixedFileUuid,
      "software",
      "softwareVersion",
      "binaryVersion",
      "containerVersion",
      "method",
      List(FFIDMetadataInputMatches(Some("ext"), "identificationBasis", Some("puid")))
    )
  }

  private def getMatchesRow(fileMetadataId: UUID) = {
    FfidmetadatamatchesRow(fileMetadataId, Some("ext"), "identificationBasis", Some("puid"))
  }

  private def getMockMetadataRow(ffidMetadataId: UUID, fixedFileUuid: UUID, dummyTimestamp: Timestamp): FfidmetadataRow = {
    FfidmetadataRow(ffidMetadataId, fixedFileUuid, "software", "softwareVersion",dummyTimestamp, "binaryVersion", "containerVersion", "method")
  }
}
