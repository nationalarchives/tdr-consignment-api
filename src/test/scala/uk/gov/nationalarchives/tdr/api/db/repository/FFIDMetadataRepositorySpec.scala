package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import java.util.UUID

class FFIDMetadataRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "countProcessedFfidMetadata" should "return 0 if consignment has no files" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("40c26c38-b6b4-4325-9d28-c9d0b50e89aa")

    utils.createConsignment(consignmentId, userId)

    val ffidMetadataFiles = ffidMetadataRepository.countProcessedFfidMetadata(consignmentId).futureValue

    ffidMetadataFiles shouldBe 0
  }

  "countProcessedFfidMetadata" should "return 0 if consignment has no FFID metadata for files" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("95dc7b75-e88b-41b4-ac65-004c61874145")
    val fileOneId = UUID.fromString("d74650ff-21b1-402d-8c59-b114698a8341")
    val fileTwoId = UUID.fromString("51c55218-1322-4453-9ef8-2300ef1c0fef")
    val fileThreeId = UUID.fromString("7076f399-b596-4161-a95d-e686c6435710")

    utils.createConsignment(consignmentId, userId)
    utils.createFile(fileOneId, consignmentId)
    utils.createFile(fileTwoId, consignmentId)
    utils.createFile(fileThreeId, consignmentId)

    val ffidMetadataFiles = ffidMetadataRepository.countProcessedFfidMetadata(consignmentId).futureValue

    ffidMetadataFiles shouldBe 0
  }

  "countProcessedFfidMetadata" should "return the number of FFIDMetadata for files in a specified consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentOne = UUID.fromString("a9ccec45-5325-4e07-a0cd-1b0f4dc0d6fd")
    val consignmentTwo = UUID.fromString("c50d3aff-1d06-4c94-9960-e7b25a882086")
    val fileOneId = "d734da24-90a7-4c05-ab37-a78746556323"
    val fileTwoId = "be77573a-8710-42a2-9a9f-522bd681d467"
    val fileThreeId = "8efd6bfb-f079-4029-9d62-2cc0f4ebe501"

    utils.createConsignment(consignmentOne, userId)
    utils.createConsignment(consignmentTwo, userId)

    utils.createFile(UUID.fromString(fileOneId), consignmentOne)
    utils.createFile(UUID.fromString(fileTwoId), consignmentOne)
    utils.createFile(UUID.fromString(fileThreeId), consignmentTwo)

    val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
    utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

    val fileMetadataFiles = ffidMetadataRepository.countProcessedFfidMetadata(consignmentOne).futureValue

    fileMetadataFiles shouldBe 2
  }

  "countProcessedFfidMetadata" should "return 0 if file has ffid metadata but no ffid matches" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignment = UUID.fromString("a9ccec45-5325-4e07-a0cd-1b0f4dc0d6fd")
    val fileId = "be77573a-8710-42a2-9a9f-522bd681d467"

    utils.createConsignment(consignment, userId)

    utils.createFile(UUID.fromString(fileId), consignment)
    utils.addFFIDMetadata(fileId)

    val fileMetadataFiles = ffidMetadataRepository.countProcessedFfidMetadata(consignment).futureValue

    fileMetadataFiles shouldBe 0
  }

  "countProcessedFfidMetadata" should "return number of ffidMetadata rows with repetitive data filtered out" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

    val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
    val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
    val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
    val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"

    utils.createConsignment(consignmentId, userId)
    utils.createFile(UUID.fromString(fileOneId), consignmentId)
    utils.createFile(UUID.fromString(fileTwoId), consignmentId)
    utils.createFile(UUID.fromString(fileThreeId), consignmentId)
    utils.createFile(UUID.fromString(fileFourId), consignmentId)

    (1 to 7).foreach { _ =>
      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)
    }

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

    val ffidMetadataFiles = ffidMetadataRepository.countProcessedFfidMetadata(consignmentId).futureValue

    ffidMetadataFiles shouldBe 3
  }

  "getFFIDMetadata" should "return the same number of ffidMetadata rows as the number of files added" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

    val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
    val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
    val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
    val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"

    utils.createConsignment(consignmentId, userId)
    utils.createFile(UUID.fromString(fileOneId), consignmentId)
    utils.createFile(UUID.fromString(fileTwoId), consignmentId)
    utils.createFile(UUID.fromString(fileThreeId), consignmentId)
    utils.createFile(UUID.fromString(fileFourId), consignmentId)

    val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
    utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

    val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId).futureValue

    ffidMetadataRows.length shouldBe 3
  }

  "getFFIDMetadata" should "return the ffidMetadata rows of the selected file ids" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

    val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
    val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
    val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"

    utils.createConsignment(consignmentId, userId)
    utils.createFile(UUID.fromString(fileOneId), consignmentId)
    utils.createFile(UUID.fromString(fileTwoId), consignmentId)
    utils.createFile(UUID.fromString(fileThreeId), consignmentId)

    val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
    utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

    val selectedFileIds: Set[UUID] = Set(UUID.fromString(fileThreeId), UUID.fromString(fileOneId))

    val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId, Some(selectedFileIds)).futureValue

    ffidMetadataRows.length shouldBe 2
    val returnedFileIds = ffidMetadataRows.map(_._1.fileid)
    returnedFileIds.contains(UUID.fromString(fileThreeId)) shouldBe true
    returnedFileIds.contains(UUID.fromString(fileOneId)) shouldBe true
  }

  "getFFIDMetadata" should "return 0 ffid metadata rows for a consignment if no files were added" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

    utils.createConsignment(consignmentId, userId)

    val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId).futureValue

    ffidMetadataRows.length shouldBe 0
  }

  "getFFIDMetadata" should "return the fileIds of the files that belong to that consignment" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val consignmentOneId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")
    val consignmentTwoId = UUID.fromString("89bdd66f-88c2-4edb-9911-2c605d002a1e")

    val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
    val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
    val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
    val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"

    utils.createConsignment(consignmentOneId, userId)
    utils.createConsignment(consignmentTwoId, userId)
    utils.createFile(UUID.fromString(fileOneId), consignmentOneId)
    utils.createFile(UUID.fromString(fileTwoId), consignmentOneId)
    utils.createFile(UUID.fromString(fileThreeId), consignmentOneId)
    utils.createFile(UUID.fromString(fileFourId), consignmentTwoId)

    val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
    utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)

    val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
    utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

    val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
    utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

    val fileFourFfidMetadataId = utils.addFFIDMetadata(fileFourId)
    utils.addFFIDMetadataMatches(fileFourFfidMetadataId.toString)

    val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentOneId).futureValue
    val fileIds: Set[UUID] = ffidMetadataRows.toMap.keySet.map(_.fileid)

    fileIds should contain(UUID.fromString(fileOneId))
    fileIds should contain(UUID.fromString(fileTwoId))
    fileIds should contain(UUID.fromString(fileThreeId))
    fileIds should not contain UUID.fromString(fileFourId)
  }

  "getFFIDMetadata" should "return only the fileIds of the files that had ffidMetadata & ffidMetadataMatches applied to them" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val ffidMetadataRepository = new FFIDMetadataRepository(db)
      val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

      val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
      val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
      val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
      val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"
      val fileFiveId = "907a6ff7-d6b5-482b-854a-0b962f8367e3"

      utils.createConsignment(consignmentId, userId)
      utils.createFile(UUID.fromString(fileOneId), consignmentId)
      utils.createFile(UUID.fromString(fileTwoId), consignmentId)
      utils.createFile(UUID.fromString(fileThreeId), consignmentId)
      utils.createFile(UUID.fromString(fileFourId), consignmentId)
      utils.createFile(UUID.fromString(fileFiveId), consignmentId)

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString)

      val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
      utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

      val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
      utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

      utils.addFFIDMetadata(fileFourId)

      val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId).futureValue
      val fileIds: Set[UUID] = ffidMetadataRows.toMap.keySet.map(_.fileid)

      fileIds should contain(UUID.fromString(fileOneId))
      fileIds should contain(UUID.fromString(fileTwoId))
      fileIds should contain(UUID.fromString(fileThreeId))
      fileIds should not contain UUID.fromString(fileFourId)
      fileIds should not contain UUID.fromString(fileFiveId)
  }

  "getFFIDMetadata" should "return files with ffidMetadata and ffidMetadataMatches that match the metadata applied to them" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val ffidMetadataRepository = new FFIDMetadataRepository(db)
      val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

      val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
      val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
      val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
      val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"

      val fileOneSoftware = "TEST DATA fileOne software"
      val fileOneSoftwareVersion = "TEST DATA fileOne software version"
      val fileOneBinarySigFileVersion = "TEST DATA fileOne binary signature file version"
      val fileOneContainerSigFileVersion = "TEST DATA fileOne container signature file version"
      val fileOneMethod = "TEST DATA fileOne method"
      val fileOneExtension = "pdf"
      val fileOneIdentificationBasis = "TEST DATA fileOne identification basis"
      val fileOnePuid = "TEST DATA fileOne puid"

      utils.createConsignment(consignmentId, userId)
      utils.createFile(UUID.fromString(fileOneId), consignmentId)
      utils.createFile(UUID.fromString(fileTwoId), consignmentId)
      utils.createFile(UUID.fromString(fileThreeId), consignmentId)
      utils.createFile(UUID.fromString(fileFourId), consignmentId)

      val fileOneFfidMetadataId =
        utils.addFFIDMetadata(fileOneId, fileOneSoftware, fileOneSoftwareVersion, fileOneBinarySigFileVersion, fileOneContainerSigFileVersion, fileOneMethod)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, fileOneExtension, fileOneIdentificationBasis, fileOnePuid)

      val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
      utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

      val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
      utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

      val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId).futureValue
      val fileOneFfidMetadata = ffidMetadataRows.head._1
      val fileOneFfidMetadataMatches = ffidMetadataRows.head._2

      fileOneFfidMetadata.binarysignaturefileversion should equal(fileOneBinarySigFileVersion)
      fileOneFfidMetadata.containersignaturefileversion should equal(fileOneContainerSigFileVersion)
      // datetime was left out, as it is generated automatically
      fileOneFfidMetadata.ffidmetadataid should equal(fileOneFfidMetadataId)
      fileOneFfidMetadata.fileid.toString should equal(fileOneId)
      fileOneFfidMetadata.method should equal(fileOneMethod)
      fileOneFfidMetadata.software should equal(fileOneSoftware)
      fileOneFfidMetadata.softwareversion should equal(fileOneSoftwareVersion)

      fileOneFfidMetadataMatches.ffidmetadataid should equal(fileOneFfidMetadataId)
      fileOneFfidMetadataMatches.extension.get should equal(fileOneExtension)
      fileOneFfidMetadataMatches.identificationbasis should equal(fileOneIdentificationBasis)
      fileOneFfidMetadataMatches.puid.get should equal(fileOnePuid)
  }

  "getFFIDMetadata" should "return multiple ffidMetadataMatches for the files that were given multiple ffidMetadataMatches" in withContainers {
    case container: PostgreSQLContainer =>
      val db = container.database
      val utils = TestUtils(db)
      val ffidMetadataRepository = new FFIDMetadataRepository(db)
      val consignmentId = UUID.fromString("21061d4d-ed73-485f-b433-c48b0868fffb")

      val fileOneId = "50f4f290-cdcc-4a0f-bd26-3bb40f320b71"
      val fileTwoId = "7f55565e-bfa5-4cf9-9e02-7e75ff033b3b"
      val fileThreeId = "89b6183d-e761-4af9-9e37-2ffa09922dba"
      val fileFourId = "939999e5-c082-4052-a99a-6b45092d826f"

      val fileOneFirstExtensionMatch = "pdf"
      val fileOneFirstIdentificationBasisMatch = "TEST DATA fileOne first identification basis"
      val fileOneFirstPuidMatch = "TEST DATA fileOne first puid"

      val fileOneSecondExtensionMatch = "png"
      val fileOneSecondIdentificationBasisMatch = "TEST DATA fileTwo second identification basis"
      val fileOneSecondPuidMatch = "TEST DATA fileTwo second puid"

      utils.createConsignment(consignmentId, userId)
      utils.createFile(UUID.fromString(fileOneId), consignmentId)
      utils.createFile(UUID.fromString(fileTwoId), consignmentId)
      utils.createFile(UUID.fromString(fileThreeId), consignmentId)
      utils.createFile(UUID.fromString(fileFourId), consignmentId)

      val fileOneFfidMetadataId = utils.addFFIDMetadata(fileOneId)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, fileOneFirstExtensionMatch, fileOneFirstIdentificationBasisMatch, fileOneFirstPuidMatch)
      utils.addFFIDMetadataMatches(fileOneFfidMetadataId.toString, fileOneSecondExtensionMatch, fileOneSecondIdentificationBasisMatch, fileOneSecondPuidMatch)

      val fileTwoFfidMetadataId = utils.addFFIDMetadata(fileTwoId)
      utils.addFFIDMetadataMatches(fileTwoFfidMetadataId.toString)

      val fileThreeFfidMetadataId = utils.addFFIDMetadata(fileThreeId)
      utils.addFFIDMetadataMatches(fileThreeFfidMetadataId.toString)

      val ffidMetadataRows = ffidMetadataRepository.getFFIDMetadata(consignmentId).futureValue
      val fileOneFirstFfidMetadataMatches = ffidMetadataRows.head._2
      val fileOneSecondFfidMetadataMatches = ffidMetadataRows(1)._2

      fileOneFirstFfidMetadataMatches.ffidmetadataid should equal(fileOneFfidMetadataId)
      fileOneFirstFfidMetadataMatches.extension.get should equal(fileOneFirstExtensionMatch)
      fileOneFirstFfidMetadataMatches.identificationbasis should equal(fileOneFirstIdentificationBasisMatch)
      fileOneFirstFfidMetadataMatches.puid.get should equal(fileOneFirstPuidMatch)

      fileOneSecondFfidMetadataMatches.ffidmetadataid should equal(fileOneFfidMetadataId)
      fileOneSecondFfidMetadataMatches.extension.get should equal(fileOneSecondExtensionMatch)
      fileOneSecondFfidMetadataMatches.identificationbasis should equal(fileOneSecondIdentificationBasisMatch)
      fileOneSecondFfidMetadataMatches.puid.get should equal(fileOneSecondPuidMatch)
  }
}
