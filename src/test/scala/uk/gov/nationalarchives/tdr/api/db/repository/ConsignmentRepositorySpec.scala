package uk.gov.nationalarchives.tdr.api.db.repository

import cats.implicits.catsSyntaxOptionId
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{
  Ascending,
  ConsignmentFilters,
  ConsignmentOrderBy,
  ConsignmentReference,
  CreatedAtTimestamp,
  Descending,
  StartUploadInput,
  UpdateClientSideDraftMetadataFileNameInput,
  UpdateExportDataInput,
  UpdateMetadataSchemaLibraryVersionInput
}
import uk.gov.nationalarchives.tdr.api.service.CurrentTimeSource
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{InProgress, Upload}
import uk.gov.nationalarchives.tdr.api.utils.Statuses.{CompletedValue, InProgressValue, MetadataReviewType}
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, TestContainerUtils, TestUtils}

import java.sql.Timestamp
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.UUID
import scala.concurrent.ExecutionContext

class ConsignmentRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentIdOne: UUID = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
  val consignmentIdTwo: UUID = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
  val consignmentIdThree: UUID = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")
  val consignmentIdFour: UUID = UUID.fromString("47019574-8407-40c7-b618-bf2b8f8b0de7")

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "updateExportData" should "update the export data for a given consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("b6da7577-3800-4ebc-821b-9d33e52def9e")
    val fixedZonedDatetime = ZonedDateTime.ofInstant(FixedTimeSource.now, ZoneOffset.UTC)
    val exportLocation = "exportLocation"
    val exportVersion = "0.0.Version"
    val updateExportDataInput = UpdateExportDataInput(consignmentId, exportLocation, Some(fixedZonedDatetime), exportVersion)
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)
    val response = consignmentRepository.updateExportData(updateExportDataInput).futureValue
    val consignmentFromDb = utils.getConsignment(consignmentId)

    response should be(1)

    consignmentFromDb.getString("ExportLocation") should equal(exportLocation)
    consignmentFromDb.getTimestamp("ExportDatetime") should equal(Timestamp.valueOf(fixedZonedDatetime.toLocalDateTime))
    consignmentFromDb.getString("ExportVersion") should equal(exportVersion)
  }

  "getParentFolder" should "get parent folder name for a consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("b6da7577-3800-4ebc-821b-9d33e52def9e")
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)
    utils.addParentFolderName(consignmentId, "TEST GET PARENT FOLDER NAME")

    val parentFolderName = consignmentRepository.getParentFolder(consignmentId).futureValue

    parentFolderName should be(Some("TEST GET PARENT FOLDER NAME"))
  }

  "getParentFolder" should "return nothing if no parent folder exists" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("8233b9a4-5c2d-4c2d-9355-e6ec5751fea5")
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)

    val parentFolderName = consignmentRepository.getParentFolder(consignmentId).futureValue

    parentFolderName should be(None)
  }

  "getSeriesOfConsignment" should "get the series for a consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("b59a8bfd-5709-46c7-a5e9-71bae146e2f1")
    val seriesId = UUID.fromString("9e2e2a51-c2d0-4b99-8bef-2ca322528861")
    val bodyId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val seriesCode = "MOCK1"
    val utils = TestUtils(db)
    utils.addTransferringBody(bodyId, "Test", "Test")
    utils.addSeries(seriesId, bodyId, seriesCode)
    utils.createConsignment(consignmentId, userId)

    val consignmentSeries = consignmentRepository.getSeriesOfConsignment(consignmentId).futureValue.head

    consignmentSeries.code should be(seriesCode)
  }

  "getTransferringBodyOfConsignment" should "get the transferring body for a consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
    val seriesId = UUID.fromString("845a4589-d412-49d7-80c6-63969112728a")
    val bodyId = UUID.fromString("edb31587-4357-4e63-b40c-75368c9d9cc9")
    val bodyName = "Some transferring body name"
    val seriesCode = "Mock series"
    val utils = TestUtils(db)
    utils.addTransferringBody(bodyId, bodyName, "some-body-code")
    utils.addSeries(seriesId, bodyId, seriesCode)
    utils.createConsignment(consignmentId, userId, seriesId, bodyId = bodyId)

    val consignmentBody = consignmentRepository.getTransferringBodyOfConsignment(consignmentId).futureValue.head

    consignmentBody.name should be(bodyName)
  }

  "getNextConsignmentSequence" should "get the next sequence ID number for a consignment row" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentIdOne = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentIdTwo = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
    val utils = TestUtils(db)
    val currentSequence: Long = consignmentRepository.getNextConsignmentSequence.futureValue
    utils.createConsignment(consignmentIdOne, userId)
    utils.createConsignment(consignmentIdTwo, userId)

    val sequenceId: Long = consignmentRepository.getNextConsignmentSequence.futureValue
    val expectedSeq = currentSequence + 3

    sequenceId should be(expectedSeq)
  }

  "getConsignment" should "return the consignment given the consignment id" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)

    val response = consignmentRepository.getConsignment(consignmentId).futureValue

    response should have size 1
    response.headOption.get.consignmentid should equal(consignmentId)
  }

  "getConsignments" should "return all consignments after the cursor up to the limit value" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(2, Some("TDR-2021-D"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    response should have size 2
    val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
    consignmentIds should contain(consignmentIdThree)
    consignmentIds should contain(consignmentIdTwo)
  }

  "getConsignments" should "return no consignments if cursor value is last one" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(10, Some("TDR-2021-A"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    response should have size 0
  }

  "getConsignments" should "return all consignments up to limit where no cursor provided including first consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(2, None, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue
    response should have size 2
    val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
    consignmentIds should contain(consignmentIdFour)
    consignmentIds should contain(consignmentIdThree)
  }

  "getConsignments" should "return no consignments when empty cursor provided" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(2, Some(""), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue
    response should have size 0
  }

  "getConsignments" should "return no consignments where limit set at '0'" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(0, Some("TDR-2021-A"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue
    response should have size 0
  }

  "getConsignments" should "return consignments where non-existent cursor value provided, and reference is lower than the cursor value" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
      val utils = TestUtils(db)
      createConsignments(utils)

      val response = consignmentRepository.getConsignments(2, Some("TDR-2021-ZZZ"), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue
      response should have size 2
      val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
      consignmentIds should contain(consignmentIdFour)
      consignmentIds should contain(consignmentIdThree)
  }

  "getConsignments" should "return no consignments where there are no consignments" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    val response = consignmentRepository.getConsignments(2, Some(""), orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue
    response should have size 0
  }

  "getConsignments" should "return all the consignments which belong to the given user id only" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")

    val user2Id: UUID = UUID.randomUUID()
    val consignmentIdForUser2: UUID = UUID.randomUUID()

    utils.createConsignment(consignmentIdForUser2, user2Id, consignmentRef = "TDR-2021-B")

    val response = consignmentRepository
      .getConsignments(10, None, consignmentFilters = ConsignmentFilters(userId.some, None).some, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))
      .futureValue

    response should have size 1
    response.map(cr => cr.consignmentid).head should equal(consignmentIdOne)
  }

  "getConsignments" should "return all the consignments which belong to the given consignment type only" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")

    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B", consignmentType = "judgment")

    val response = consignmentRepository
      .getConsignments(10, None, consignmentFilters = ConsignmentFilters(None, "judgment".some).some, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending))
      .futureValue

    response should have size 1
    response.map(cr => cr.consignmentid).head should equal(consignmentIdTwo)
  }

  "getConsignments" should "return all the consignments for all the users when user id is not passed" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")

    val user2Id: UUID = UUID.randomUUID()
    val consignmentIdForUser2: UUID = UUID.randomUUID()

    utils.createConsignment(consignmentIdForUser2, user2Id, consignmentRef = "TDR-2021-B")

    val response = consignmentRepository.getConsignments(10, None, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    response should have size 2
    val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
    consignmentIds should contain(consignmentIdOne)
    consignmentIds should contain(consignmentIdForUser2)
  }

  "getConsignments" should "return the consignments in descending order" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(10, None, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    val consignmentReferences: List[String] = response.map(cr => cr.consignmentreference).toList

    response should have size 4
    consignmentReferences should equal(List("TDR-2021-D", "TDR-2021-C", "TDR-2021-B", "TDR-2021-A"))
  }

  "getConsignments" should "return all consignments up to the limit, when current page is provided" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(2, None, 1.some, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    val consignmentReferences: List[String] = response.map(cr => cr.consignmentreference).toList

    response should have size 2
    consignmentReferences should equal(List("TDR-2021-B", "TDR-2021-A"))
  }

  "getConsignmentsForMetadataReview" should "return the consignments with `MetadataReview` status set to `InProgress`" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
    val consignmentId2 = UUID.fromString("dc6ca08d-6dd6-4906-8f07-97f4f6492dfc")
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)
    utils.createConsignment(consignmentId2, userId)
    utils.createConsignmentStatus(consignmentId, MetadataReviewType.id, InProgressValue.value)
    utils.createConsignmentStatus(consignmentId2, MetadataReviewType.id, CompletedValue.value)

    val response = consignmentRepository.getConsignmentsForMetadataReview.futureValue

    response should have size 1
    response.headOption.get.consignmentid should equal(consignmentId)
  }

  "getConsignmentForMetadataReview" should "return the matching consignment when the `MetadataReview` status set to `InProgress`" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
      val db = container.database
      val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
      val utils = TestUtils(db)
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, MetadataReviewType.id, InProgressValue.value)

      val response = consignmentRepository.getConsignmentForMetadataReview(consignmentId).futureValue

      response should have size 1
      response.headOption.get.consignmentid should equal(consignmentId)
  }

  "getConsignmentForMetadataReview" should "not return the matching consignment when the 'MetadataReview' status is not `InProgress`" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
      val db = container.database
      val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
      val utils = TestUtils(db)
      utils.createConsignment(consignmentId, userId)
      utils.createConsignmentStatus(consignmentId, MetadataReviewType.id, CompletedValue.value)

      val response = consignmentRepository.getConsignmentForMetadataReview(consignmentId).futureValue

      response.isEmpty should be(true)
  }

  "updateSeriesOfConsignment" should "update id and name of the consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    val seriesId: UUID = UUID.fromString("20e88b3c-d063-4a6e-8b61-187d8c51d11d")
    val seriesName: String = "Mock1"
    val bodyId: UUID = UUID.fromString("8a72cc59-7f2f-4e55-a263-4a4cb9f677f5")

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")
    utils.addTransferringBody(bodyId, "MOCK Department", "Code123")
    utils.addSeries(seriesId, bodyId, "TDR-2020-XYZ", seriesName)

    val input = ConsignmentFields.UpdateConsignmentSeriesIdInput(consignmentId = consignmentIdOne, seriesId = seriesId)

    val response = consignmentRepository.updateSeriesOfConsignment(input, seriesName.some).futureValue

    response should be(1)
    val consignment = consignmentRepository.getConsignment(consignmentIdOne).futureValue.head
    consignment.seriesid should be(seriesId.some)
    consignment.seriesname should be(seriesName.some)
  }

  "updateMetadataSchemaLibraryVersionOfConsignment" should "update the validation schema library version of the consignment" in withContainers {
    case container: PostgreSQLContainer =>
      val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
      val db = container.database
      val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
      val utils = TestUtils(db)
      utils.createConsignment(consignmentId, userId)
      val version = "3.4.5"

      val response = consignmentRepository.updateMetadataSchemaLibraryVersion(UpdateMetadataSchemaLibraryVersionInput(consignmentId, version)).futureValue

      response should be(1)
      val consignment = consignmentRepository.getConsignment(consignmentId).futureValue.head
      consignment.metadataschemalibraryversion should be(version.some)
  }

  "updateClientSideDraftMetadataFileName" should "update the draft metadata file name for the consignment" in withContainers { case container: PostgreSQLContainer =>
    val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    utils.createConsignment(consignmentId, userId)
    val fileName = "a-filename.csv"

    val response = consignmentRepository.updateClientSideDraftMetadataFileName(UpdateClientSideDraftMetadataFileNameInput(consignmentId, fileName)).futureValue

    response should be(1)
    val consignment = consignmentRepository.getConsignment(consignmentId).futureValue.head
    consignment.clientsidedraftmetadatafilename should be(fileName.some)
  }

  "totalConsignments" should "return total number of consignments" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    val expectedItems = createConsignments(utils) + createStandardConsignmentsForReportingUser(utils)

    val response = consignmentRepository.getTotalConsignments(None).futureValue

    response should be(expectedItems)
  }

  "totalConsignments" should "return total number of consignments which belong to given userId only" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    val expectedItems = createStandardConsignmentsForReportingUser(utils) + createJudgmentConsignmentsForReportingUser(utils)
    createConsignments(utils)

    val response = consignmentRepository.getTotalConsignments(ConsignmentFilters(reportingUser.some, None).some).futureValue

    response should be(expectedItems)
  }

  "totalConsignments" should "return total number of consignments which belong to given userId and consignment type" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    val expectedItems = createJudgmentConsignmentsForReportingUser(utils)
    createStandardConsignmentsForReportingUser(utils)
    createConsignments(utils)

    val response = consignmentRepository.getTotalConsignments(ConsignmentFilters(reportingUser.some, "judgment".some).some).futureValue

    response should be(expectedItems)
  }

  "totalConsignments" should "return total number of consignments which belong to given consignment type only" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    val expectedItems = createConsignments(utils) + createStandardConsignmentsForReportingUser(utils)
    createJudgmentConsignmentsForReportingUser(utils)

    val response = consignmentRepository.getTotalConsignments(ConsignmentFilters(None, "standard".some).some).futureValue

    response should be(expectedItems)
  }

  "addUploadDetails" should "add upload details and consignment statuses" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val startUploadInput = StartUploadInput(consignmentIdOne, "parentFolder", true)

    val consignmentStatusUploadRow = ConsignmentstatusRow(consignmentIdOne, startUploadInput.consignmentId, Upload, InProgress, Timestamp.from(Instant.now()))
    val response = consignmentRepository.addUploadDetails(startUploadInput, List(consignmentStatusUploadRow)).futureValue

    response should be(startUploadInput.parentFolder)
    val consignment = consignmentRepository.getConsignment(consignmentIdOne).futureValue
    consignment.isEmpty should not be (true)
    consignment.head.parentfolder.get should be(startUploadInput.parentFolder)
    consignment.head.includetoplevelfolder.get should be(startUploadInput.includeTopLevelFolder)

    val consignmentStatusFromDb = utils.getConsignmentStatus(consignmentIdOne, Upload)
    consignmentStatusFromDb.getString("Value") should be(InProgress)
  }

  "getConsignments" should "return consignments ordered by consignmentReference ascending" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-C")
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-A")
    utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-B")

    val orderBy = ConsignmentOrderBy(ConsignmentReference, Ascending)
    val response = consignmentRepository.getConsignments(10, None, None, None, orderBy).futureValue

    val consignmentReferences = response.map(_.consignmentreference).toList
    consignmentReferences should equal(List("TDR-2021-A", "TDR-2021-B", "TDR-2021-C"))
  }

  "getConsignments" should "return consignments ordered by createdDatetime descending" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A", timestamp = Timestamp.from(Instant.parse("2024-01-01T10:00:00Z")))
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B", timestamp = Timestamp.from(Instant.parse("2024-01-03T10:00:00Z")))
    utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-C", timestamp = Timestamp.from(Instant.parse("2024-01-02T10:00:00Z")))

    val orderBy = ConsignmentOrderBy(CreatedAtTimestamp, Descending)
    val response = consignmentRepository.getConsignments(10, None, None, None, orderBy).futureValue

    val consignmentReferences = response.map(_.consignmentreference).toList
    consignmentReferences should equal(List("TDR-2021-B", "TDR-2021-C", "TDR-2021-A"))
  }

  "getConsignments" should "correctly apply cursor filter with timestamp ordering ascending" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A", timestamp = Timestamp.from(Instant.parse("2024-01-01T10:00:00Z")))
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B", timestamp = Timestamp.from(Instant.parse("2024-01-02T10:00:00Z")))
    utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-C", timestamp = Timestamp.from(Instant.parse("2024-01-03T10:00:00Z")))

    val orderBy = ConsignmentOrderBy(CreatedAtTimestamp, Ascending)
    val cursor = Some("2024-01-01 10:00:00")
    val response = consignmentRepository.getConsignments(10, cursor, None, None, orderBy).futureValue

    response.map(_.consignmentreference).toList should equal(List("TDR-2021-B", "TDR-2021-C"))
  }

  "getConsignments" should "correctly apply cursor filter with timestamp ordering descending" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A", timestamp = Timestamp.from(Instant.parse("2024-01-01T10:00:00Z")))
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B", timestamp = Timestamp.from(Instant.parse("2024-01-02T10:00:00Z")))
    utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-C", timestamp = Timestamp.from(Instant.parse("2024-01-03T10:00:00Z")))

    val orderBy = ConsignmentOrderBy(CreatedAtTimestamp, Descending)
    val cursor = Some("2024-01-02 10:00:00")
    val response = consignmentRepository.getConsignments(10, cursor, None, None, orderBy).futureValue

    response.map(_.consignmentreference).toList should equal(List("TDR-2021-A"))
  }

  "getConsignments" should "correctly apply cursor filter with consignment reference ordering descending" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B")
    utils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-C")

    val orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)
    val cursor = Some("TDR-2021-C")
    val response = consignmentRepository.getConsignments(10, cursor, None, None, orderBy).futureValue

    response.map(_.consignmentreference).toList should equal(List("TDR-2021-B", "TDR-2021-A"))
  }

  "getConsignments" should "combine ordering by consignment reference ascending with filters correctly" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)

    utils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")
    utils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-C")

    val otherUserId = UUID.randomUUID()
    utils.createConsignment(consignmentIdThree, otherUserId, consignmentRef = "TDR-2021-B")

    val filters = Some(ConsignmentFilters(Some(userId), None))
    val orderBy = ConsignmentOrderBy(ConsignmentReference, Ascending)
    val response = consignmentRepository.getConsignments(10, None, None, filters, orderBy).futureValue

    response.map(_.consignmentreference).toList should equal(List("TDR-2021-A", "TDR-2021-C"))
  }

  "getConsignments" should "use default ordering when orderBy is not specified" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val utils = TestUtils(db)
    createConsignments(utils)

    val response = consignmentRepository.getConsignments(10, None, None, None, orderBy = ConsignmentOrderBy(ConsignmentReference, Descending)).futureValue

    val consignmentReferences = response.map(_.consignmentreference).toList
    consignmentReferences should equal(List("TDR-2021-D", "TDR-2021-C", "TDR-2021-B", "TDR-2021-A"))
  }

  private def createConsignments(utils: TestUtils): Int = {
    val consignments = Map(consignmentIdOne -> "TDR-2021-A", consignmentIdTwo -> "TDR-2021-B", consignmentIdThree -> "TDR-2021-C", consignmentIdFour -> "TDR-2021-D")
    consignments.foreach(item => utils.createConsignment(item._1, userId, consignmentRef = item._2))
    consignments.size
  }

  private def createStandardConsignmentsForReportingUser(utils: TestUtils): Int = {
    val consignments = Map(UUID.randomUUID() -> "TDR-2022-A", UUID.randomUUID() -> "TDR-2022-B")
    consignments.foreach(item => utils.createConsignment(item._1, reportingUser, consignmentRef = item._2))
    consignments.size
  }

  private def createJudgmentConsignmentsForReportingUser(utils: TestUtils): Int = {
    val consignments = Map(UUID.randomUUID() -> "TDR-2022-C", UUID.randomUUID() -> "TDR-2022-D")
    consignments.foreach(item => utils.createConsignment(item._1, reportingUser, consignmentRef = item._2, consignmentType = "judgment"))
    consignments.size
  }
}
