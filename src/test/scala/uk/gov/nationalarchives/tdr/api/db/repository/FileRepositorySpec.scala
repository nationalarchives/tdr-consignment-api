package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestUtils}

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext


class FileRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addFiles" should "create files and update ConsignmentStatus table for a consignment" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("94abafc4-165e-469b-ba93-eace3f224de5")
    val fileOneId = UUID.fromString("7499a278-2fec-4c47-92fb-dd9024c65d0d")
    val fileTwoId = UUID.fromString("e7d21444-0c62-4115-a4ad-320fd3d3dae3")
    val fileRows = Seq(
      FileRow(fileOneId, consignmentId, userId, Timestamp.from(Instant.now)),
      FileRow(fileTwoId, consignmentId, userId, Timestamp.from(Instant.now))
    )
    val consignmentStatusRow = ConsignmentstatusRow(
      UUID.fromString("ad5ac54c-6a67-4892-b8ac-120362df7917"),
      consignmentId,
      "Upload",
      "InProgress",
      Timestamp.from(Instant.now)
    )

    TestUtils.createConsignment(consignmentId, userId)

    val addFiles: Seq[nationalarchives.Tables.FileRow] = fileRepository.addFiles(fileRows, consignmentStatusRow).futureValue

    addFiles.foreach{ file =>
      file.consignmentid shouldBe consignmentId
      file.userid shouldBe userId
    }
    checkConsignmentStatusExists(consignmentId)
  }

  "countFilesInConsignment" should "return 0 if a consignment has no files" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("c03fd4be-58c1-4cee-8d3c-d162bb4f7c02")

    TestUtils.createConsignment(consignmentId, userId)

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId).futureValue

    consignmentFiles shouldBe 0
  }

  "countFilesInConsignment" should "return the total number of files in a consignment" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("2e31c0ce-25e6-4bd7-a8a7-dc8bbb9335ba")

    TestUtils.createConsignment(consignmentId, userId)

    TestUtils.createFile(UUID.fromString("4bde68aa-6212-45dc-9097-769b9f77dbd9"), consignmentId)
    TestUtils.createFile(UUID.fromString("d870fb86-0dd5-4025-98d3-11232690918b"), consignmentId)
    TestUtils.createFile(UUID.fromString("2dfa0495-72a3-4e88-9c0e-b105d7802a4e"), consignmentId)
    TestUtils.createFile(UUID.fromString("1ad53749-aba4-4369-8fd6-2311111427cc"), consignmentId)

    val consignmentFiles = fileRepository.countFilesInConsignment(consignmentId).futureValue

    consignmentFiles shouldBe 4
  }

  "countProcessedAvMetadataInConsignment" should "return 0 if consignment has no AVmetadata for files" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("64456c78-49bb-4bff-85c8-2fff053b9f8e")

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createFile(UUID.fromString("2c2dedf5-56a1-497f-9f8a-19102739a055"), consignmentId)
    TestUtils.createFile(UUID.fromString("c2d5c569-0fc4-4688-9523-157c4028f1b0"), consignmentId)
    TestUtils.createFile(UUID.fromString("317c7084-d3d4-435b-acd2-cfb317793843"), consignmentId)
//  We have created files, but not added any AVmetadata for those files to the AVMetadata repository.
//  Thus, calling the countProcessedAvMetadataInConsignment method should return 0.

    val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(consignmentId).futureValue

    avMetadataFiles shouldBe 0
  }

  "countProcessedAvMetadataInConsignment" should "return the number of AVmetadata for files in a specified consignment" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentOne = "049a11d7-06f5-4b11-b786-640000de76e1"
    val consignmentTwo = "dc2acd2c-3370-44fe-aeae-16aee30dc41f"
    val fileOneId = "5a102380-25ec-4881-a484-87d56fe9a0b3"
    val fileTwoId = "f616272d-4d80-44cf-90de-996c3984847c"
    val fileThreeId = "40c67a87-70c0-4424-b934-a395459ddbe0"

//  Need to create a consignment with files in it
    TestUtils.createConsignment(UUID.fromString(consignmentOne), userId)
    TestUtils.createConsignment(UUID.fromString(consignmentTwo), userId)
    TestUtils.createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentOne))
    TestUtils.createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentOne))
    TestUtils.createFile(UUID.fromString(fileThreeId), UUID.fromString(consignmentTwo))

//  Then need to add data to the AVMetadata repository for these files
    TestUtils.addAntivirusMetadata(fileOneId)
    TestUtils.addAntivirusMetadata(fileTwoId)
    TestUtils.addAntivirusMetadata(fileThreeId)

    val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(UUID.fromString(consignmentOne)).futureValue

    avMetadataFiles shouldBe 2
  }

  "countProcessedAvMetadataInConsignment" should "return number of AVmetadata rows with repetitive data filtered out" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = "c6f78fef-704a-46a8-82c0-afa465199e65"
    val fileOneId = "20e0676a-f0a1-4051-9540-e7df1344ac10"
    val fileTwoId = "b5111f11-4dca-4f92-8239-505da567b9df"

    TestUtils.createConsignment(UUID.fromString(consignmentId), userId)
    TestUtils.createFile(UUID.fromString(fileOneId), UUID.fromString(consignmentId))
    TestUtils.createFile(UUID.fromString(fileTwoId), UUID.fromString(consignmentId))

    (1 to 7).foreach { _ => TestUtils.addAntivirusMetadata(fileOneId)}

    TestUtils.addAntivirusMetadata(fileTwoId)
    val avMetadataFiles = fileRepository.countProcessedAvMetadataInConsignment(UUID.fromString(consignmentId)).futureValue

    avMetadataFiles shouldBe 2
  }

  "getFilesWithPassedAntivirus" should "return only files where the antivirus has found no virus" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("dba4515f-474c-4a5a-a297-260b6ba1ffa3")
    val fileOneId = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")
    val fileTwoId = UUID.fromString("53d2927e-89dd-48a8-bb19-d33b7baa4e44")

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createFile(fileOneId, consignmentId)
    TestUtils.createFile(fileTwoId, consignmentId)

    TestUtils.addAntivirusMetadata(fileOneId.toString, "")
    TestUtils.addAntivirusMetadata(fileTwoId.toString)
    val files = fileRepository.getFilesWithPassedAntivirus(consignmentId).futureValue

    files.size shouldBe 1
    files.head.fileid shouldBe fileOneId
  }

  "getConsignmentForFile" should "return the correct consignment for the given file id" in {
    val db = DbConnection.db
    val fileRepository = new FileRepository(db)
    val consignmentId = UUID.fromString("dba4515f-474c-4a5a-a297-260b6ba1ffa3")
    val fileOneId = UUID.fromString("92756098-b394-4f46-8b4d-bbd1953660c9")

    TestUtils.createConsignment(consignmentId, userId)
    TestUtils.createFile(fileOneId, consignmentId)

    val files = fileRepository.getConsignmentForFile(fileOneId).futureValue

    files.size shouldBe 1
    files.head.consignmentid shouldBe consignmentId
  }

  private def checkConsignmentStatusExists(consignmentId: UUID): Unit = {
    val sql = "SELECT * FROM ConsignmentStatus WHERE ConsignmentId = ?"
    val ps:PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, consignmentId.toString)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
    rs.getString("ConsignmentId") should equal(consignmentId.toString)
    rs.next() should equal (false)
  }
}
