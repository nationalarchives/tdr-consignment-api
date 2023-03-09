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
