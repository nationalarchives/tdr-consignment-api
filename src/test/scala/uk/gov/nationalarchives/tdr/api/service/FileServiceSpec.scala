package uk.gov.nationalarchives.tdr.api.service

import cats.implicits.catsSyntaxOptionId
import com.typesafe.config.ConfigFactory
import org.mockito.ArgumentMatchers._
import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.Tables.{
  AvmetadataRow,
  ConsignmentRow,
  FfidmetadataRow,
  FfidmetadatamatchesRow,
  FileRow,
  FilemetadataRow,
  FilepropertyRow,
  FilepropertyvaluesRow,
  FilestatusRow
}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.FileFields
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.QueriedFileFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.PaginationInput
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, ClientSideMetadataInput, FileMatches}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class FileServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val uuidSource: FixedUUIDSource = new FixedUUIDSource()

  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val customMetadataPropertiesRepositoryMock: CustomMetadataPropertiesRepository = mock[CustomMetadataPropertiesRepository]
  val customMetadataServiceMock: CustomMetadataPropertiesService = mock[CustomMetadataPropertiesService]
  val fileMetadataRepositoryMock: FileMetadataRepository = mock[FileMetadataRepository]
  val fileRepositoryMock: FileRepository = mock[FileRepository]
  val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
  val ffidMetadataRepositoryMock: FFIDMetadataRepository = mock[FFIDMetadataRepository]
  val antivirusMetadataRepositoryMock: AntivirusMetadataRepository = mock[AntivirusMetadataRepository]
  val referenceGeneratorServiceMock: ReferenceGeneratorService = mock[ReferenceGeneratorService]
  val fileStatusServiceMock: FileStatusService = mock[FileStatusService]
  val consignmentStatusService = new ConsignmentStatusService(consignmentStatusRepositoryMock, uuidSource, FixedTimeSource)
  val fileMetadataService =
    new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
  val queriedFileFieldsWithoutOriginalPath: QueriedFileFields = QueriedFileFields()

  "getOwnersOfFiles" should "find the owners of the given files" in {
    val fixedUuidSource = new FixedUUIDSource()
    val fileId1 = UUID.fromString("bc609dc4-e153-4620-a7ab-20e7fd5a4005")
    val fileId2 = UUID.fromString("67178a08-36ea-41c2-83ee-4b343b6429cb")
    val userId1 = UUID.fromString("e9cac50f-c5eb-42b4-bb5d-355ccf8920cc")
    val userId2 = UUID.fromString("f4ffe1d0-3525-4a7c-ba0c-812f6e054ab1")
    val consignmentId1 = UUID.fromString("0ae52efa-4f01-4b05-84f1-e36626180dad")

    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val fileService = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    when(fileRepositoryMock.getFileFields(Set(fileId1)))
      .thenReturn(
        Future.successful(
          Seq((fileId1, Some(NodeType.fileTypeIdentifier), userId1, consignmentId1, None), (fileId2, Some(NodeType.fileTypeIdentifier), userId2, consignmentId1, None))
        )
      )
    val mockFileMetadataResponse = Future.successful(Seq(FilemetadataRow(UUID.randomUUID(), fileId1, "value", Timestamp.from(Instant.now), userId1, "name")))
    when(fileMetadataRepositoryMock.addFileMetadata(any[Seq[AddFileMetadataInput]])).thenReturn(mockFileMetadataResponse)

    val owners = fileService.getOwnersOfFiles(Seq(fileId1)).futureValue

    owners should have size 2

    owners.head.fileId should equal(fileId1)
    owners.head.userId should equal(userId1)
    owners(1).fileId should equal(fileId2)
    owners(1).userId should equal(userId2)
  }

  "getFileDetails" should "return all the correct files details from the database response" in {
    val fileRepositoryMock = mock[FileRepository]
    val fixedUuidSource = new FixedUUIDSource()
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()

    val folderFields = new FileFields(parentFolderId, Some(NodeType.directoryTypeIdentifier), userId, consignmentId, None)
    val fileOneFields = new FileFields(fileIdOne, Some(NodeType.fileTypeIdentifier), userId, consignmentId, Some("1"))
    val fileTwoFields = new FileFields(fileIdTwo, Some(NodeType.fileTypeIdentifier), userId, consignmentId, Some("2"))
    when(fileRepositoryMock.getFileFields(Set(fileIdOne, fileIdTwo, parentFolderId)))
      .thenReturn(Future(Seq(folderFields, fileOneFields, fileTwoFields)))

    val service = new FileService(
      fileRepositoryMock,
      mock[CustomMetadataPropertiesRepository],
      mock[FFIDMetadataService],
      mock[AntivirusMetadataService],
      mock[FileStatusService],
      mock[FileMetadataService],
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val response = service.getFileDetails(Seq(fileIdTwo, fileIdOne, parentFolderId)).futureValue
    response.size shouldBe 3

    val parentFolder = response.find(_.fileId == parentFolderId).get
    parentFolder.fileType.get should equal(NodeType.directoryTypeIdentifier)
    parentFolder.userId should equal(userId)
    parentFolder.consignmentId should equal(consignmentId)
    parentFolder.uploadMatchId shouldBe None

    val fileOne = response.find(_.fileId == fileIdOne).get
    fileOne.fileType.get should equal(NodeType.fileTypeIdentifier)
    fileOne.userId should equal(userId)
    fileOne.consignmentId should equal(consignmentId)
    fileOne.uploadMatchId.get should equal("1")

    val fileTwo = response.find(_.fileId == fileIdTwo).get
    fileTwo.fileType.get should equal(NodeType.fileTypeIdentifier)
    fileTwo.userId should equal(userId)
    fileTwo.consignmentId should equal(consignmentId)
    fileTwo.uploadMatchId.get should equal("2")
  }

  "getFileMetadata" should "return all the correct files and folders with the correct metadata from the database response" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val timestamp = Timestamp.from(FixedTimeSource.now)
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val fileIdOne = UUID.randomUUID()
    val fileIdTwo = UUID.randomUUID()
    val fileIdThree = UUID.randomUUID()

    val parentFolderRow = FileRow(parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))
    val fileOneRow = FileRow(
      fileIdOne,
      consignmentId,
      userId,
      timestamp,
      Some(true),
      Some(NodeType.fileTypeIdentifier),
      Some("fileOneName"),
      Some(parentFolderId),
      uploadmatchid = Some("1")
    )
    val fileTwoRow = FileRow(
      fileIdTwo,
      consignmentId,
      userId,
      timestamp,
      Some(true),
      Some(NodeType.fileTypeIdentifier),
      Some("fileTwoName"),
      Some(parentFolderId),
      uploadmatchid = Some("2")
    )
    val fileThreeRow = FileRow(
      fileIdThree,
      consignmentId,
      userId,
      timestamp,
      Some(true),
      Some(NodeType.fileTypeIdentifier),
      Some("fileThreeName"),
      Some(parentFolderId),
      uploadmatchid = Some("3")
    )

    val fileAndMetadataRows: Seq[(FileRow, Option[FilemetadataRow])] = Seq(
      (fileOneRow, Some(fileMetadataRow(fileIdOne, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileOneRow, Some(fileMetadataRow(fileIdOne, "SHA256ClientSideChecksum", "checksum"))),
      (fileTwoRow, Some(fileMetadataRow(fileIdTwo, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileTwoRow, Some(fileMetadataRow(fileIdTwo, "SHA256ClientSideChecksum", "checksum"))),
      (fileThreeRow, Some(fileMetadataRow(fileIdThree, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (parentFolderRow, None)
    )
    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), "FFID", "Success", timestamp))
    )

    when(fileRepositoryMock.getFiles(consignmentId, FileFilters(None)))
      .thenReturn(Future(fileAndMetadataRows))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(List()))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID))).thenReturn(mockFileStatusResponse)

    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService =
      new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val files = service.getFileMetadata(consignmentId, queriedFileFields = queriedFileFieldsWithoutOriginalPath).futureValue
    files.size shouldBe 4

    val parentFolder = files.find(_.fileId == parentFolderId).get
    parentFolder.fileName.get shouldBe "folderName"
    parentFolder.uploadMatchId shouldBe None
    parentFolder.metadata shouldBe FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None)

    val fileOne = files.find(_.fileId == fileIdOne).get
    fileOne.fileName.get shouldBe "fileOneName"
    fileOne.uploadMatchId.get shouldBe "1"
    fileOne.metadata shouldBe FileMetadataValues(Some("checksum"), None, Some(timestamp.toLocalDateTime), None, None, None, None, None, None, None, None, None, None, None)
    fileOne.originalFilePath.isDefined should be(false)

    val fileTwo = files.find(_.fileId == fileIdTwo).get
    fileTwo.fileName.get shouldBe "fileTwoName"
    fileTwo.uploadMatchId.get shouldBe "2"
    fileTwo.metadata shouldBe FileMetadataValues(Some("checksum"), None, Some(timestamp.toLocalDateTime), None, None, None, None, None, None, None, None, None, None, None)
    fileTwo.originalFilePath.isDefined should be(false)

    val fileThree = files.find(_.fileId == fileIdThree).get
    fileThree.fileName.get shouldBe "fileThreeName"
    fileThree.uploadMatchId.get shouldBe "3"
    fileThree.metadata shouldBe FileMetadataValues(None, None, Some(timestamp.toLocalDateTime), None, None, None, None, None, None, None, None, None, None, None)
    fileThree.originalFilePath.isDefined should be(false)
  }

  val filterMetadata: TableFor1[String] = Table(
    "MetadataType",
    "closureMetadata",
    "descriptiveMetadata",
    "closureMetadata & descriptiveMetadata"
  )

  forAll(filterMetadata) { metadataType =>
    "getFileMetadata" should s"return filtered fileMetadata when fileMetadatFilter $metadataType and properties are passed" in {
      val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
      val fileRepositoryMock = mock[FileRepository]
      val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
      val fixedUuidSource = new FixedUUIDSource()

      val timestamp = Timestamp.from(FixedTimeSource.now)
      val consignmentId = UUID.randomUUID()
      val parentFolderId = UUID.randomUUID()
      val fileIdOne = UUID.randomUUID()
      val parentFolderRow = FileRow(parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))
      val fileOneRow = FileRow(
        fileIdOne,
        consignmentId,
        userId,
        timestamp,
        Some(true),
        Some(NodeType.fileTypeIdentifier),
        Some("fileOneName"),
        Some(parentFolderId),
        uploadmatchid = Some("1")
      )

      val fileAndMetadataRows: Seq[(FileRow, Option[FilemetadataRow])] = Seq(
        (fileOneRow, Some(fileMetadataRow(fileIdOne, "ClosureType", "Open"))),
        (fileOneRow, Some(fileMetadataRow(fileIdOne, "ClosurePeriod", "12"))),
        (fileOneRow, Some(fileMetadataRow(fileIdOne, "Property1", "Property1Value"))),
        (fileOneRow, Some(fileMetadataRow(fileIdOne, "Property2", "Property2Value"))),
        (parentFolderRow, None)
      )
      val mockFileStatusResponse = Future(
        Seq(FilestatusRow(UUID.randomUUID(), UUID.randomUUID(), "FFID", "Success", timestamp))
      )
      val mockPropertyResponse = Future(
        Seq(
          FilepropertyRow(
            "ClosureType",
            None,
            Some("Closure Type"),
            Timestamp.from(Instant.now()),
            None,
            Some("Defined"),
            Some("text"),
            Some(true),
            None,
            Some("MandatoryClosure")
          ),
          FilepropertyRow(
            "ClosurePeriod",
            None,
            Some("Closure Period"),
            Timestamp.from(Instant.now()),
            None,
            Some("Defined"),
            Some("text"),
            Some(true),
            None,
            Some("OptionalClosure")
          ),
          FilepropertyRow(
            "Property1",
            None,
            Some("PropertyName1"),
            Timestamp.from(Instant.now()),
            None,
            Some("Defined"),
            Some("text"),
            Some(true),
            None,
            Some("MandatoryMetadata")
          ),
          FilepropertyRow("Property2", None, Some("PropertyName2"), Timestamp.from(Instant.now()), None, Some("Defined"), Some("text"), Some(true), None, Some("OptionalMetadata"))
        )
      )
      val metadataFilters = Map(
        "closureMetadata" -> FileMetadataFilters(closureMetadata = true, properties = List("Property1").some).some,
        "descriptiveMetadata" -> FileMetadataFilters(descriptiveMetadata = true).some,
        "closureMetadata & descriptiveMetadata" -> FileMetadataFilters(closureMetadata = true, descriptiveMetadata = true).some
      )

      val expectedMetadata = Map(
        "closureMetadata" -> (List("ClosureType", "ClosurePeriod", "Property1"), List("Open", "12", "Property1Value")),
        "descriptiveMetadata" -> (List("Property1", "Property2"), List("Property1Value", "Property2Value")),
        "closureMetadata & descriptiveMetadata" -> (List("ClosureType", "ClosurePeriod", "Property1", "Property2"), List("Open", "12", "Property1Value", "Property2Value"))
      )

      val fileFilters = FileFilters(metadataFilters = metadataFilters(metadataType))

      when(fileRepositoryMock.getFiles(consignmentId, fileFilters))
        .thenReturn(Future(fileAndMetadataRows))
      when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(List()))
      when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))
      when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID))).thenReturn(mockFileStatusResponse)
      when(customMetadataPropertiesRepositoryMock.getCustomMetadataProperty).thenReturn(mockPropertyResponse)

      val ffidMetadataService = new FFIDMetadataService(
        ffidMetadataRepositoryMock,
        mock[FFIDMetadataMatchesRepository],
        FixedTimeSource,
        fixedUuidSource
      )
      val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
      val fileStatusService =
        new FileStatusService(fileStatusRepositoryMock)

      val service = new FileService(
        fileRepositoryMock,
        customMetadataPropertiesRepositoryMock,
        ffidMetadataService,
        antivirusMetadataService,
        fileStatusService,
        fileMetadataService,
        referenceGeneratorServiceMock,
        FixedTimeSource,
        fixedUuidSource,
        ConfigFactory.load()
      )

      val files = service.getFileMetadata(consignmentId, fileFilters.some, queriedFileFieldsWithoutOriginalPath).futureValue
      val file = files.find(_.fileId == fileIdOne).get
      file.fileMetadata.size should equal(expectedMetadata(metadataType)._1.size)
      file.fileMetadata.map(_.name) should equal(expectedMetadata(metadataType)._1)
      file.fileMetadata.map(_.value) should equal(expectedMetadata(metadataType)._2)
    }
  }

  "getFileMetadata" should "return the correct metadata with file statuses" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val fileRef = "FILEREF"
    val parentId = UUID.randomUUID()
    val parentRef = "REF1"
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()
    val closureStartDate = Timestamp.from(Instant.parse("2020-03-01T09:00:00Z"))
    val foiExemptionAsserted = Timestamp.from(Instant.parse("2020-04-01T09:00:00Z"))

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))

    val fileRow =
      FileRow(
        fileId,
        consignmentId,
        userId,
        timestamp,
        Some(true),
        Some(NodeType.fileTypeIdentifier),
        Some("fileName"),
        Some(parentId),
        Some(fileRef),
        Some(parentRef),
        uploadmatchid = Some("1")
      )

    val fileAndMetadataRows = Seq(
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideFileLastModifiedDate", timestamp.toString))),
      (fileRow, Some(fileMetadataRow(fileId, "SHA256ClientSideChecksum", "checksum"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideOriginalFilepath", "filePath"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClientSideFileSize", "1"))),
      (fileRow, Some(fileMetadataRow(fileId, "RightsCopyright", "rightsCopyright"))),
      (fileRow, Some(fileMetadataRow(fileId, "LegalStatus", "legalStatus"))),
      (fileRow, Some(fileMetadataRow(fileId, "HeldBy", "heldBy"))),
      (fileRow, Some(fileMetadataRow(fileId, "Language", "language"))),
      (fileRow, Some(fileMetadataRow(fileId, "FoiExemptionCode", "foiExemption"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClosurePeriod", "0"))),
      (fileRow, Some(fileMetadataRow(fileId, "ClosureStartDate", closureStartDate.toString))),
      (fileRow, Some(fileMetadataRow(fileId, "FoiExemptionAsserted", foiExemptionAsserted.toString))),
      (fileRow, Some(fileMetadataRow(fileId, "TitleClosed", "true"))),
      (fileRow, Some(fileMetadataRow(fileId, DescriptionClosed, "true")))
    )

    val mockAvMetadataResponse = Future(
      Seq(AvmetadataRow(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp))
    )

    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(), fileId, "FFID", "Success", timestamp))
    )
    val mockFileStatuses =
      Seq(FilestatusRow(UUID.randomUUID(), fileId, "FFID", "Success", timestamp))

    val allFileStatusTypes: Set[String] = Set(ChecksumMatch, Antivirus, FFID, Redaction, Upload, ServerChecksum, ClientChecks)

    when(fileRepositoryMock.getFiles(consignmentId, FileFilters()))
      .thenReturn(Future(fileAndMetadataRows))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(mockAvMetadataResponse)
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), None)).thenReturn(mockFileStatusResponse)
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, allFileStatusTypes, None)).thenReturn(Future(mockFileStatuses))

    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )
    val queriedFileFields = QueriedFileFields(antivirusMetadata = true, ffidMetadata = true, fileStatus = true, fileStatuses = true)
    val fileList: Seq[File] = service.getFileMetadata(consignmentId, queriedFileFields = queriedFileFields).futureValue

    fileList.length should equal(1)

    val actualFileMetadata: File = fileList.head
    val fileMetadata = fileAndMetadataRows.map(row => FileMetadataValue(row._2.get.propertyname, row._2.get.value)).toList
    val expectedFileMetadata = File(
      fileId,
      Some("1"),
      Some(NodeType.fileTypeIdentifier),
      Some("fileName"),
      Some(fileRef),
      Some(parentId),
      Some(parentRef),
      FileMetadataValues(
        Some("checksum"),
        Some("filePath"),
        Some(timestamp.toLocalDateTime),
        Some(1),
        Some("rightsCopyright"),
        Some("legalStatus"),
        Some("heldBy"),
        Some("language"),
        Some("foiExemption"),
        Some("0"),
        Some(closureStartDate.toLocalDateTime),
        Some(foiExemptionAsserted.toLocalDateTime),
        Some(true),
        Some(true)
      ),
      Some("Success"),
      Some(
        FFIDMetadata(
          fileId,
          "pronom",
          "1.0",
          "signaturefileversion",
          "signature",
          "pronom",
          List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"), Some(false), Some("formatName"))),
          datetime.getTime
        )
      ),
      Option(AntivirusMetadata(fileId, "software", "softwareVersion", "databaseVersion", "result", timestamp.getTime)),
      None,
      fileMetadata,
      mockFileStatuses.map(p => FileStatus(p.fileid, p.statustype, p.value)).toList
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "getFileMetadata" should "return empty fields if the metadata has an unexpected property name and no file data" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val fileRepositoryMock = mock[FileRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val fileId = UUID.randomUUID()
    val datetime = Timestamp.from(Instant.now())
    val ffidMetadataId = UUID.randomUUID()

    val ffidMetadataRows = Seq(
      (ffidMetadataRow(ffidMetadataId, fileId, datetime), ffidMetadataMatchesRow(ffidMetadataId))
    )

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(ffidMetadataRows))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))

    val mockFileStatusResponse = Future(
      Seq(FilestatusRow(UUID.randomUUID(), fileId, "FFID", "Success", datetime))
    )

    val fileRow = FileRow(fileId, consignmentId, userId, Timestamp.from(Instant.now))
    val fileAndMetadataRows = Seq(
      (fileRow, Some(fileMetadataRow(fileId, "customPropertyNameOne", "customValueOne"))),
      (fileRow, Some(fileMetadataRow(fileId, "customPropertyNameTwo", "customValueTwo")))
    )
    when(fileRepositoryMock.getFiles(consignmentId, FileFilters(None))).thenReturn(Future(fileAndMetadataRows))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID))).thenReturn(mockFileStatusResponse)

    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val queriedFileFields = QueriedFileFields(antivirusMetadata = true, ffidMetadata = true, fileStatus = true)
    val fileMetadataList = service.getFileMetadata(consignmentId, queriedFileFields = queriedFileFields).futureValue

    fileMetadataList.length should equal(1)

    val actualFileMetadata = fileMetadataList.head
    val fileMetadata = fileAndMetadataRows.map(row => FileMetadataValue(row._2.get.propertyname, row._2.get.value)).toList
    val expectedFileMetadata = File(
      fileId,
      None,
      None,
      None,
      None,
      None,
      None,
      FileMetadataValues(None, None, None, None, None, None, None, None, None, None, None, None, None, None),
      Some("Success"),
      Some(
        FFIDMetadata(
          fileId,
          "pronom",
          "1.0",
          "signaturefileversion",
          "signature",
          "pronom",
          List(FFIDMetadataMatches(Some("txt"), "identification", Some("x-fmt/111"), Some(false), Some("formatName"))),
          datetime.getTime
        )
      ),
      Option.empty,
      None,
      fileMetadata
    )

    actualFileMetadata should equal(expectedFileMetadata)
  }

  "getFileMetadata" should "return the original file if the file is a redacted file" in {
    val ffidMetadataRepositoryMock = mock[FFIDMetadataRepository]
    val antivirusRepositoryMock = mock[AntivirusMetadataRepository]
    val fileStatusRepositoryMock: FileStatusRepository = mock[FileStatusRepository]
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val redactedFileId = UUID.randomUUID()

    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId)).thenReturn(Future(Seq()))
    when(antivirusRepositoryMock.getAntivirusMetadata(consignmentId)).thenReturn(Future(List()))

    val redactedFileRow = FileRow(redactedFileId, consignmentId, userId, Timestamp.from(Instant.now))
    val originalPath = "/an/original/path"

    val redactedFileMetadataRow: FilemetadataRow =
      FilemetadataRow(UUID.randomUUID(), redactedFileId, originalPath, Timestamp.from(FixedTimeSource.now), userId, "OriginalFilepath")
    val fileAndMetadataRows = Seq((redactedFileRow, Option(redactedFileMetadataRow)))

    when(fileRepositoryMock.getFiles(consignmentId, FileFilters(None))).thenReturn(Future(fileAndMetadataRows))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID))).thenReturn(Future(Seq()))

    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val queriedFieldsWithOriginalPath = QueriedFileFields(originalFilePath = true)
    val fileMetadataList = service.getFileMetadata(consignmentId, queriedFileFields = queriedFieldsWithOriginalPath).futureValue

    fileMetadataList.length should equal(1)

    val actualFileMetadata = fileMetadataList.filter(_.fileId == redactedFileId).head

    actualFileMetadata.originalFilePath.get should equal(originalPath)
  }

  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are greater than batch size" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileStatusServiceMock = mock[FileStatusService]
    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val file1Id = UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f")
    val file2Id = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    when(fileStatusServiceMock.addFileStatuses(any[AddMultipleFileStatusesInput])).thenReturn(Future(Nil))
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3", "ref4", "ref5"))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusServiceMock,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val input = setupMetadataInput(consignmentId)
    val response = service.addFile(input, userId).futureValue

    verify(fileRepositoryMock, times(2)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(file1Id)
    response.head.matchId should equal("2")

    response.last.fileId should equal(file2Id)
    response.last.matchId should equal("1")

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 61
    metadataRows.size should equal(expectedSize)
    defaultMetadataProperties.foreach(prop => {
      metadataRows.count(_.propertyname == prop) should equal(5)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType | FileReference | ParentReference => count should equal(5) // Directories have this set
        case _                                                                                  => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "addFile" should "assign file references to all files, directories and add all files, directories, client and static metadata when total number of files and folders are greater than batch size" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileStatusServiceMock = mock[FileStatusService]
    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val file1Id = UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f")
    val file2Id = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    when(fileStatusServiceMock.addFileStatuses(any[AddMultipleFileStatusesInput])).thenReturn(Future(Nil))
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3", "ref4", "ref5"))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusServiceMock,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val input = setupMetadataInput(consignmentId)

    val response = service.addFile(input, userId).futureValue

    verify(fileRepositoryMock, times(2)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(file1Id)
    response.head.matchId should equal("2")

    response.last.fileId should equal(file2Id)
    response.last.matchId should equal("1")

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
      row.filereference should be(defined)
    })
    val file = fileRows.find(_.filereference.contains("ref4"))
    file.get.parentreference should equal(Some("ref2"))
    val expectedSize = 61
    metadataRows.size should equal(expectedSize)
    defaultMetadataProperties.foreach(prop => {
      metadataRows.count(_.propertyname == prop) should equal(5)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType | FileReference | ParentReference => count should equal(5) // Directories have this set
        case _                                                                                  => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "addFile" should "add all files, directories, client and static metadata when total number of files and folders are less than batch size" in {
    val consignmentId = UUID.randomUUID()
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataServiceMock = mock[CustomMetadataPropertiesService]
    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val fixedUuidSource = new FixedUUIDSource()

    val userId = UUID.randomUUID()

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    val metadataInputOne = ClientSideMetadataInput("/a/OriginalPath1", "Checksum1", 1L, 1L, "1")
    val metadataInputTwo = ClientSideMetadataInput("", "", 1L, 1L, "2")

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))

    when(fileStatusServiceMock.addFileStatuses(any[AddMultipleFileStatusesInput])).thenAnswer { invocation: org.mockito.invocation.InvocationOnMock =>
      val input = invocation.getArgument[AddMultipleFileStatusesInput](0)
      val fileStatuses = input.statuses.map(statusInput => FileStatus(statusInput.fileId, statusInput.statusType, statusInput.statusValue))
      Future.successful(fileStatuses)
    }

    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3"))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusServiceMock,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val response = service.addFile(AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo)), userId).futureValue

    verify(fileRepositoryMock, times(1)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.head.matchId should equal("1")

    response.last.fileId should equal(UUID.fromString("8e3b76c4-1745-4467-8ac5-b4dd736e1b3e"))
    response.last.matchId should equal("2")

    val expectedFileRows = 3
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(userId)
    })
    val expectedSize = 39
    metadataRows.size should equal(expectedSize)
    defaultMetadataProperties.foreach(prop => {
      metadataRows.count(_.propertyname == prop) should equal(3)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType | FileReference | ParentReference => count should equal(3) // Directories have this set
        case _                                                                                  => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "addFile" should "set user id to override id where present on input for metadata entries" in {
    val ffidMetadataService = mock[FFIDMetadataService]
    val antivirusMetadataService = mock[AntivirusMetadataService]
    val fileRepositoryMock = mock[FileRepository]
    val customMetadataPropertiesRepositoryMock = mock[CustomMetadataPropertiesRepository]
    val fileStatusServiceMock = mock[FileStatusService]
    val fileMetadataService =
      new FileMetadataService(fileMetadataRepositoryMock, customMetadataServiceMock)
    val fixedUuidSource = new FixedUUIDSource()

    val consignmentId = UUID.randomUUID()
    val tokenUserId = UUID.randomUUID()
    val overrideUserId = UUID.randomUUID()
    val file1Id = UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f")
    val file2Id = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")

    val fileRowCaptor: ArgumentCaptor[List[FileRow]] = ArgumentCaptor.forClass(classOf[List[FileRow]])
    val metadataRowCaptor: ArgumentCaptor[List[FilemetadataRow]] = ArgumentCaptor.forClass(classOf[List[FilemetadataRow]])

    when(fileRepositoryMock.addFiles(fileRowCaptor.capture(), metadataRowCaptor.capture())).thenReturn(Future(()))
    when(referenceGeneratorServiceMock.getReferences(any[Int])).thenReturn(List("ref1", "ref2", "ref3", "ref4", "ref5"))
    mockCustomMetadataValuesResponse(customMetadataPropertiesRepositoryMock)

    val service = new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusServiceMock,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )

    val input = setupMetadataInput(consignmentId, Some(overrideUserId))
    val response = service.addFile(input, tokenUserId).futureValue

    verify(fileRepositoryMock, times(2)).addFiles(any[List[FileRow]](), any[List[FilemetadataRow]]())

    val fileRows: List[FileRow] = fileRowCaptor.getAllValues.asScala.flatten.toList
    val metadataRows: List[FilemetadataRow] = metadataRowCaptor.getAllValues.asScala.flatten.toList

    response.head.fileId should equal(file1Id)
    response.head.matchId should equal("2")

    response.last.fileId should equal(file2Id)
    response.last.matchId should equal("1")

    val expectedFileRows = 5
    fileRows.size should equal(expectedFileRows)
    fileRows.foreach(row => {
      row.consignmentid should equal(consignmentId)
      row.userid should equal(overrideUserId)
    })
    val expectedSize = 61
    metadataRows.size should equal(expectedSize)
    defaultMetadataProperties.foreach(prop => {
      metadataRows.count(_.propertyname == prop) should equal(5)
    })

    clientSideProperties.foreach(prop => {
      val count = metadataRows.count(r => r.propertyname == prop)
      prop match {
        case ClientSideOriginalFilepath | Filename | FileType | FileReference | ParentReference => count should equal(5) // Directories have this set
        case _                                                                                  => count should equal(2)
      }
    })
    verify(consignmentStatusRepositoryMock, times(0)).updateConsignmentStatus(any[UUID], any[String], any[String], any[Timestamp])
  }

  "getPaginatedFiles" should "return all the file edges after the cursor to the limit" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId, Some("2")),
      (fileId3, consignmentId, "fileName3", parentId, Some("3"))
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4, p._5))
    val limit = 2
    val page = 0
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(None, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, page, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[FileMetadataService.File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2
    firstEdge.node.parentId.get shouldBe parentId
    firstEdge.node.fileType.get shouldBe NodeType.fileTypeIdentifier
    firstEdge.node.fileName.get shouldBe "fileName2"
    firstEdge.node.uploadMatchId.get shouldBe "2"
    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
    secondEdge.node.parentId.get shouldBe parentId
    secondEdge.node.fileType.get shouldBe NodeType.fileTypeIdentifier
    secondEdge.node.fileName.get shouldBe "fileName3"
    secondEdge.node.uploadMatchId.get shouldBe "3"
  }

  "getPaginatedFiles" should "return all the files edges after the cursor to the maximum limit where the requested limit is greater than the maximum" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limitExceedingMax = 3
    val expectedMaxLimit = 2
    val currentPage = 0
    val offset = 0
    val input = Some(PaginationInput(Some(limitExceedingMax), Some(currentPage), Some(fileId1), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(Some(consignmentId), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, expectedMaxLimit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return all the file edges up to the limit where no cursor provided" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limit = 2
    val page = 0
    val offset = 0
    val input = Some(PaginationInput(Some(limit), Some(page), None, None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(Some(consignmentId), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, None, FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe false

    edges.size shouldBe 2
    response.totalItems shouldBe 2

    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return all the file edges up to the limit where filters provided" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileId2 = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val fileId3 = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")

    val fileRowParams = List(
      (fileId2, consignmentId, "fileName2", parentId),
      (fileId3, consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))

    val limit = 2
    val page = 0
    val offset = 0
    val fileFilters = FileFilters(Some(NodeType.fileTypeIdentifier))
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), Some(fileFilters)))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)
    val selectedFileIds: Option[Set[UUID]] = Some(Set(fileId2, fileId3))

    when(fileMetadataRepositoryMock.getFileMetadata(Some(consignmentId), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, fileFilters.fileTypeIdentifier))
      .thenReturn(Future.successful(2))

    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), fileFilters))
      .thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue
    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor.get shouldBe fileId2.toString
    pageInfo.endCursor.get shouldBe fileId3.toString
    pageInfo.hasNextPage shouldBe true
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 2
    response.totalItems shouldBe 2

    val firstEdge = edges.head
    firstEdge.cursor shouldBe fileId2.toString
    firstEdge.node.fileId shouldBe fileId2

    val secondEdge = edges.last
    secondEdge.cursor shouldBe fileId3.toString
    secondEdge.node.fileId shouldBe fileId3
  }

  "getPaginatedFiles" should "return no files edges if no files exist" in {
    val consignmentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileRepositoryMock = mock[FileRepository]
    val limit = 2
    val page = 0
    val offset = 0
    val mockResponse: Future[Seq[FileRow]] = Future.successful(Seq())
    val selectedFileIds: Option[Set[UUID]] = Some(Set())

    when(fileMetadataRepositoryMock.getFileMetadata(None, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(consignmentId, selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(consignmentId, Set(FFID), selectedFileIds)).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(consignmentId, None, None)).thenReturn(Future.successful(0))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val input = Some(PaginationInput(Some(limit), Some(page), Some(fileId1), None))

    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val pageInfo = response.pageInfo
    val edges = response.edges

    pageInfo.startCursor shouldBe None
    pageInfo.endCursor shouldBe None
    pageInfo.hasNextPage shouldBe false
    pageInfo.hasPreviousPage shouldBe true

    edges.size shouldBe 0
    response.totalItems shouldBe 0
  }

  "getPaginatedFiles" should "return an error if no pagination input argument provided" in {
    val consignmentId = UUID.randomUUID()
    val fileId1 = "bc609dc4-e153-4620-a7ab-20e7fd5a4005"
    val fileRepositoryMock = mock[FileRepository]
    val limit = 2
    val offset = 0
    val mockResponse: Future[Seq[FileRow]] = Future.successful(Seq())
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, limit, offset, Some(fileId1), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val thrownException = intercept[Exception] {
      fileService.getPaginatedFiles(consignmentId, None, queriedFileFieldsWithoutOriginalPath).futureValue
    }

    thrownException.getMessage should equal("No pagination input argument provided for 'paginatedFiles' field query")
  }

  "getPaginatedFiles" should "return all the files by natural sorting order" in {
    val fileRepositoryMock = mock[FileRepository]
    val consignmentId = UUID.randomUUID()
    val parentId = UUID.randomUUID()

    val fileRowParams = List(
      (UUID.randomUUID(), consignmentId, "fileName2", parentId),
      (UUID.randomUUID(), consignmentId, "fileName22", parentId),
      (UUID.randomUUID(), consignmentId, "fileName21", parentId),
      (UUID.randomUUID(), consignmentId, "fileName31", parentId),
      (UUID.randomUUID(), consignmentId, "fileName32", parentId),
      (UUID.randomUUID(), consignmentId, "fileName1", parentId),
      (UUID.randomUUID(), consignmentId, "fileName5", parentId),
      (UUID.randomUUID(), consignmentId, "fileName", parentId),
      (UUID.randomUUID(), consignmentId, "fileName3", parentId)
    )

    val fileRows: List[FileRow] = fileRowParams.map(p => createFileRow(p._1, p._2, p._3, p._4))
    val limit = 100
    val page = 0
    val input = Some(PaginationInput(Some(limit), Some(page), Some(parentId.toString), None))

    val mockResponse: Future[Seq[FileRow]] = Future.successful(fileRows)

    when(fileMetadataRepositoryMock.getFileMetadata(ArgumentMatchers.eq(None), any[Option[Set[UUID]]], any[Option[Set[String]]]))
      .thenReturn(Future.successful(Seq()))
    when(ffidMetadataRepositoryMock.getFFIDMetadata(ArgumentMatchers.eq(consignmentId), any[Option[Set[UUID]]]())).thenReturn(Future.successful(Seq()))
    when(antivirusMetadataRepositoryMock.getAntivirusMetadata(ArgumentMatchers.eq(consignmentId), any())).thenReturn(Future.successful(Seq()))
    when(fileStatusRepositoryMock.getFileStatus(ArgumentMatchers.eq(consignmentId), ArgumentMatchers.eq(Set(FFID)), any())).thenReturn(Future.successful(Seq()))
    when(fileRepositoryMock.countFilesInConsignment(ArgumentMatchers.eq(consignmentId), any(), any())).thenReturn(Future.successful(8))
    when(fileRepositoryMock.getPaginatedFiles(consignmentId, 2, page, Some(parentId.toString), FileFilters())).thenReturn(mockResponse)

    val fileService = setupFileService(fileRepositoryMock)
    val response: TDRConnection[File] = fileService.getPaginatedFiles(consignmentId, input, queriedFileFieldsWithoutOriginalPath).futureValue

    val edges = response.edges

    edges.size shouldBe fileRowParams.size
    edges.map(_.node.fileName.getOrElse("")) should equal(
      List("fileName", "fileName1", "fileName2", "fileName3", "fileName5", "fileName21", "fileName22", "fileName31", "fileName32")
    )
  }

  "getConsignmentParentFolderId" should "return the parent folder id for a given consignment" in {
    val consignmentId = UUID.randomUUID()
    val parentFolderId = UUID.randomUUID()
    val timestamp = Timestamp.from(FixedTimeSource.now)
    val parentFolderRow = FileRow(parentFolderId, consignmentId, userId, timestamp, Some(true), Some(NodeType.directoryTypeIdentifier), Some("folderName"))

    val fileService = setupFileService(fileRepositoryMock)
    when(fileRepositoryMock.getConsignmentParentFolder(consignmentId)).thenReturn(Future.successful(Seq(parentFolderRow)))

    val parentFolderIdResult: Option[UUID] = fileService.getConsignmentParentFolderId(consignmentId).futureValue
    parentFolderIdResult.get shouldBe parentFolderId
  }

  "getConsignmentParentFolderId" should "return None if the parent folder does not exist for a given consignment" in {
    val consignmentId = UUID.randomUUID()
    when(fileRepositoryMock.getConsignmentParentFolder(consignmentId)).thenReturn(Future.successful(Seq()))

    val fileService = setupFileService(fileRepositoryMock)
    val parentFolderIdResult: Option[UUID] = fileService.getConsignmentParentFolderId(consignmentId).futureValue
    parentFolderIdResult shouldBe None
  }

  private def setupFileService(fileRepositoryMock: FileRepository): FileService = {
    val fixedUuidSource = new FixedUUIDSource()
    val ffidMetadataService = new FFIDMetadataService(
      ffidMetadataRepositoryMock,
      mock[FFIDMetadataMatchesRepository],
      FixedTimeSource,
      fixedUuidSource
    )
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepositoryMock, fixedUuidSource, FixedTimeSource)
    val fileStatusService = new FileStatusService(fileStatusRepositoryMock)

    new FileService(
      fileRepositoryMock,
      customMetadataPropertiesRepositoryMock,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorServiceMock,
      FixedTimeSource,
      fixedUuidSource,
      ConfigFactory.load()
    )
  }

  private def setupMetadataInput(consignmentId: UUID, userId: Option[UUID] = None): AddFileAndMetadataInput = {
    val metadataInputOne = ClientSideMetadataInput("/a/nested/path/OriginalPath1", "Checksum1", 1L, 1L, "1")
    val metadataInputTwo = ClientSideMetadataInput("OriginalPath2", "Checksum2", 1L, 1L, "2")
    AddFileAndMetadataInput(consignmentId, List(metadataInputOne, metadataInputTwo), userIdOverride = userId)
  }

  private def setupExpectedStatusInput(fileIds: Set[UUID]): AddMultipleFileStatusesInput = {
    val inputs: List[AddFileStatusInput] = fileIds
      .flatMap(id => {
        List(
          AddFileStatusInput(id, "ClosureMetadata", "NotEntered"),
          AddFileStatusInput(id, "DescriptiveMetadata", "NotEntered")
        )
      })
      .toList

    AddMultipleFileStatusesInput(inputs)
  }

  private def ffidMetadataRow(ffidMetadataid: UUID, fileId: UUID, datetime: Timestamp): FfidmetadataRow =
    FfidmetadataRow(ffidMetadataid, fileId, "pronom", "1.0", datetime, "signaturefileversion", "signature", "pronom")

  private def ffidMetadataMatchesRow(ffidMetadataid: UUID): FfidmetadatamatchesRow =
    FfidmetadatamatchesRow(ffidMetadataid, Some("txt"), "identification", Some("x-fmt/111"), Some(false), Some("formatName"))

  private def fileMetadataRow(fileId: UUID, propertyName: String, value: String): FilemetadataRow =
    FilemetadataRow(UUID.randomUUID(), fileId, value, Timestamp.from(Instant.now()), UUID.randomUUID(), propertyName)

  private def createFileRow(id: UUID, consignmentId: UUID, fileName: String, parentId: UUID, uploadMatchId: Option[String] = None): FileRow = {
    FileRow(
      id,
      consignmentId,
      userId,
      Timestamp.from(FixedTimeSource.now),
      Some(true),
      Some(NodeType.fileTypeIdentifier),
      Some(fileName),
      Some(parentId),
      uploadmatchid = uploadMatchId
    )
  }

  private def mockCustomMetadataValuesResponse(customMetadataMock: CustomMetadataPropertiesRepository): ScalaOngoingStubbing[Future[Seq[FilepropertyvaluesRow]]] = {
    val defaultMetadataRows = defaultMetadataProperties.map(defaultMetadata => {
      FilepropertyvaluesRow(
        defaultMetadata,
        defaultMetadata match {
          case RightsCopyright  => defaultCopyright
          case LegalStatus      => defaultLegalStatus
          case HeldBy           => defaultHeldBy
          case Language         => defaultLanguage
          case FoiExemptionCode => defaultFoiExemptionCode
        },
        Some(true)
      )
    })

    when(customMetadataMock.getCustomMetadataValuesWithDefault).thenReturn(Future(defaultMetadataRows))
  }
}
