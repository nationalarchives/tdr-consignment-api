package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers.any
import org.mockito.scalatest.ResetMocksAfterEachTest
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.Tables.ConsignmentstatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.CurrentStatus
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.{ConsignmentStatus, ConsignmentStatusInput}
import uk.gov.nationalarchives.tdr.api.service.ConsignmentStatusService.{validStatusTypes, validStatusValues}
import uk.gov.nationalarchives.tdr.api.utils.{FixedTimeSource, FixedUUIDSource}

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentStatusServiceSpec extends AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest with Matchers with ScalaFutures with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentStatusRepositoryMock: ConsignmentStatusRepository = mock[ConsignmentStatusRepository]
  val consignmentService = new ConsignmentStatusService(consignmentStatusRepositoryMock, new FixedUUIDSource(), FixedTimeSource)

  val statusTypes: TableFor1[String] = Table(
    "Types",
    "Series",
    "TransferAgreement",
    "Upload",
    "ConfirmTransfer",
    "Export",
    "ClientChecks",
    "ServerAntivirus",
    "ServerChecksum",
    "ServerFFID"
  )

  val statusValues: TableFor1[String] = Table(
    "Value",
    "Completed",
    "CompletedWithIssues",
    "InProgress",
    "Failed"
  )

  "addConsignmentStatus" should "pass the correct consignment status and value to the repository method" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val expectedStatusValue = "Completed"

    val consignmentStatusRowCaptor: ArgumentCaptor[ConsignmentstatusRow] = ArgumentCaptor.forClass(classOf[ConsignmentstatusRow])
    val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    val mockGetConsignmentStatusRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future(Seq())
    val mockAddConsignmentStatusRepoResponse = Future(
      generateConsignmentStatusRow(expectedConsignmentId, expectedStatusType, expectedStatusValue, None)
    )

    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentIdCaptor.capture())).thenReturn(mockGetConsignmentStatusRepoResponse)
    when(consignmentStatusRepositoryMock.addConsignmentStatus(consignmentStatusRowCaptor.capture())).thenReturn(mockAddConsignmentStatusRepoResponse)

    val addConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))
    consignmentService.addConsignmentStatus(addConsignmentStatusInput).futureValue

    val consignmentStatusRowPassedToRepo = consignmentStatusRowCaptor.getValue

    consignmentIdCaptor.getValue should equal(expectedConsignmentId)
    consignmentStatusRowPassedToRepo.consignmentid should equal(expectedConsignmentId)
    consignmentStatusRowPassedToRepo.statustype should equal(expectedStatusType)
    consignmentStatusRowPassedToRepo.value should equal(expectedStatusValue)
  }

  "addConsignmentStatus" should "return the consignment status row if a row with same statusType doesn't already exist" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val expectedStatusValue = "Completed"
    val consignmentStatusRow = generateConsignmentStatusRow(
      expectedConsignmentId,
      expectedStatusType,
      expectedStatusValue,
      None
    )

    val mockGetConsignmentStatusRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future(Seq())
    val mockAddConsignmentStatusRepoResponse = Future(consignmentStatusRow)

    when(consignmentStatusRepositoryMock.getConsignmentStatus(any[UUID])).thenReturn(mockGetConsignmentStatusRepoResponse)
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(mockAddConsignmentStatusRepoResponse)

    val addConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val response: ConsignmentStatus = consignmentService.addConsignmentStatus(addConsignmentStatusInput).futureValue

    response.consignmentId should equal(expectedConsignmentId)
    response.statusType should equal(expectedStatusType)
    response.value should equal(expectedStatusValue)
  }

  "addConsignmentStatus" should "throw an exception if row with same statusType already exists" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val initialStatusValue = "InProgress"
    val expectedStatusValue = "Completed"
    val initialConsignmentStatusRow = generateConsignmentStatusRow(
      expectedConsignmentId,
      expectedStatusType,
      initialStatusValue,
      None
    )
    val consignmentStatusRow = generateConsignmentStatusRow(
      expectedConsignmentId,
      expectedStatusType,
      expectedStatusValue,
      None
    )

    val mockGetConsignmentStatusRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future(Seq(initialConsignmentStatusRow))
    val mockAddConsignmentStatusRepoResponse = Future(consignmentStatusRow)

    when(consignmentStatusRepositoryMock.getConsignmentStatus(any[UUID])).thenReturn(mockGetConsignmentStatusRepoResponse)
    when(consignmentStatusRepositoryMock.addConsignmentStatus(any[ConsignmentstatusRow])).thenReturn(mockAddConsignmentStatusRepoResponse)

    val addConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.addConsignmentStatus(addConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(
      "The future returned an exception of type: " +
        "uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException, " +
        s"with message: Existing consignment $expectedStatusType status is '$initialStatusValue'; new entry cannot be added."
    )
  }

  "addConsignmentStatus" should "throw an exception if an incorrect statusType has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "Completed"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.addConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
  }

  "addConsignmentStatus" should "throw an exception if an incorrect statusValue has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Upload"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.addConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
  }

  "addConsignmentStatus" should "throw an exception if an incorrect statusType and statusValue have been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.addConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'")
  }

  "getConsignmentStatuses" should "return all consignment statuses" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid
    val zoneId = ZoneId.of("UTC")
    val consignmentStatusRow1 = generateConsignmentStatusRow(consignmentId, "Series", "Completed")
    val consignmentStatusRow2 = generateConsignmentStatusRow(consignmentId, "TransferAgreement", "Completed")
    val consignmentStatusRow3 = generateConsignmentStatusRow(consignmentId, "Upload", "Completed")

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] =
      Future.successful(
        Seq(
          consignmentStatusRow1,
          consignmentStatusRow2,
          consignmentStatusRow3
        )
      )
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: List[ConsignmentStatus] = consignmentService.getConsignmentStatuses(consignmentId).futureValue
    response.size shouldBe 3
    val seriesStatus = response.find(_.statusType == "Series").get
    seriesStatus.value shouldBe "Completed"
    seriesStatus.consignmentId should equal(consignmentId)
    seriesStatus.consignmentStatusId should equal(consignmentStatusRow1.consignmentstatusid)
    seriesStatus.createdDatetime should equal(ZonedDateTime.ofInstant(consignmentStatusRow1.createddatetime.toInstant, zoneId))
    seriesStatus.modifiedDatetime.get should equal(ZonedDateTime.ofInstant(consignmentStatusRow1.modifieddatetime.get.toInstant, zoneId))

    val taStatus = response.find(_.statusType == "TransferAgreement").get
    taStatus.value shouldBe "Completed"
    taStatus.consignmentId should equal(consignmentId)
    taStatus.consignmentStatusId should equal(consignmentStatusRow2.consignmentstatusid)
    taStatus.createdDatetime should equal(ZonedDateTime.ofInstant(consignmentStatusRow2.createddatetime.toInstant, zoneId))
    taStatus.modifiedDatetime.get should equal(ZonedDateTime.ofInstant(consignmentStatusRow2.modifieddatetime.get.toInstant, zoneId))

    val uploadStatus = response.find(_.statusType == "Upload").get
    uploadStatus.value shouldBe "Completed"
    uploadStatus.consignmentId should equal(consignmentId)
    uploadStatus.consignmentStatusId should equal(consignmentStatusRow3.consignmentstatusid)
    uploadStatus.createdDatetime should equal(ZonedDateTime.ofInstant(consignmentStatusRow3.createddatetime.toInstant, zoneId))
    uploadStatus.modifiedDatetime.get should equal(ZonedDateTime.ofInstant(consignmentStatusRow3.modifieddatetime.get.toInstant, zoneId))
  }

  "getConsignmentStatuses" should "not return any available consignment statuses if there are no consignment statuses" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val consignmentId = fixedUUIDSource.uuid

    val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] =
      Future.successful(
        Seq()
      )
    when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

    val response: List[ConsignmentStatus] = consignmentService.getConsignmentStatuses(consignmentId).futureValue
    response.isEmpty shouldBe true
  }

  "getConsignmentStatus" should
    "return a CurrentStatus object, where all properties have a value, if all consignment status exists for given consignment" in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid
      val consignmentStatusRow1 = generateConsignmentStatusRow(consignmentId, "Series", "Completed")
      val consignmentStatusRow2 = generateConsignmentStatusRow(consignmentId, "TransferAgreement", "Completed")
      val consignmentStatusRow3 = generateConsignmentStatusRow(consignmentId, "Upload", "Completed")
      val consignmentStatusRow4 = generateConsignmentStatusRow(consignmentId, "ClientChecks", "Completed")
      val consignmentStatusRow5 = generateConsignmentStatusRow(consignmentId, "ServerAntivirus", "Completed")
      val consignmentStatusRow6 = generateConsignmentStatusRow(consignmentId, "ServerChecksum", "Completed")
      val consignmentStatusRow7 = generateConsignmentStatusRow(consignmentId, "ServerFFID", "Completed")
      val consignmentStatusRow8 = generateConsignmentStatusRow(consignmentId, "ConfirmTransfer", "Completed")
      val consignmentStatusRow9 = generateConsignmentStatusRow(consignmentId, "Export", "Completed")

      val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] =
        Future.successful(
          Seq(
            consignmentStatusRow1,
            consignmentStatusRow2,
            consignmentStatusRow3,
            consignmentStatusRow4,
            consignmentStatusRow5,
            consignmentStatusRow6,
            consignmentStatusRow7,
            consignmentStatusRow8,
            consignmentStatusRow9
          )
        )
      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

      val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response.series should be(Some("Completed"))
      response.transferAgreement should be(Some("Completed"))
      response.upload should be(Some("Completed"))
      response.clientChecks should be(Some("Completed"))
      response.serverAntivirus should be(Some("Completed"))
      response.serverChecksum should be(Some("Completed"))
      response.serverFFID should be(Some("Completed"))
      response.confirmTransfer should be(Some("Completed"))
      response.export should be(Some("Completed"))
    }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the series property has a value, while others have a value of 'None',
      | if only the series status exists for given consignment""".stripMargin in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid

      val mockConsignmentStatusResponse = Future.successful(
        Seq(
          generateConsignmentStatusRow(consignmentId, "Series", "Complete")
        )
      )

      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockConsignmentStatusResponse)

      val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response.series should be(Some("Complete"))
      response.transferAgreement should be(None)
      response.upload should be(None)
      response.confirmTransfer should be(None)
    }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the transferAgreement property has a value, while others have a value of 'None',
      | if only the transferAgreement consignment status exists for given consignment""".stripMargin in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid
      val consignmentStatusRow = generateConsignmentStatusRow(consignmentId, "TransferAgreement", "Completed")

      val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow))
      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

      val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response.series should be(None)
      response.transferAgreement should be(Some("Completed"))
      response.upload should be(None)
      response.confirmTransfer should be(None)
    }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the upload property has a value, while others have a value of 'None',
      | if only one consignment status exists for given consignment""".stripMargin in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid
      val dateTime = Timestamp.from(FixedTimeSource.now)

      val mockConsignmentStatusResponse = Future.successful(
        Seq(
          generateConsignmentStatusRow(consignmentId, "Upload", "InProgress")
        )
      )

      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockConsignmentStatusResponse)

      val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response.series should be(None)
      response.transferAgreement should be(None)
      response.upload should be(Some("InProgress"))
      response.confirmTransfer should be(None)
    }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where only the confirmTransfer property has a value, while others have a value of 'None',
      | if only one consignment status exists for given consignment""".stripMargin in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid
      val consignmentStatusRow = generateConsignmentStatusRow(consignmentId, "ConfirmTransfer", "Completed")

      val mockRepoResponse: Future[Seq[ConsignmentstatusRow]] = Future.successful(Seq(consignmentStatusRow))
      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

      val response: CurrentStatus = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response.series should be(None)
      response.transferAgreement should be(None)
      response.upload should be(None)
      response.confirmTransfer should be(Some("Completed"))
    }

  "getConsignmentStatus" should
    """return a CurrentStatus object, where all properties have a value of 'None',
      | if consignment statuses don't exist for given consignment""".stripMargin in {
      val fixedUUIDSource = new FixedUUIDSource()
      val consignmentId = fixedUUIDSource.uuid
      val mockRepoResponse = Future.successful(Seq())
      when(consignmentStatusRepositoryMock.getConsignmentStatus(consignmentId)).thenReturn(mockRepoResponse)

      val response = consignmentService.getConsignmentStatus(consignmentId).futureValue

      response should be(CurrentStatus(None, None, None, None, None, None, None, None, None))
    }

  forAll(statusValues) { statusValue =>
    "updateConsignmentStatus" should s"pass the correct consignment status and value '$statusValue' to the repository method" in {
      val fixedUUIDSource = new FixedUUIDSource()
      val expectedConsignmentId = fixedUUIDSource.uuid
      val expectedStatusType = "TransferAgreement"
      val expectedStatusValue = statusValue
      val modifiedTime = Timestamp.from(FixedTimeSource.now)
      val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
      val statusTypeCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val statusValueCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val modifiedTimeCaptor: ArgumentCaptor[Timestamp] = ArgumentCaptor.forClass(classOf[Timestamp])

      val consignmentStatusMockRepoResponse: Future[Int] = Future.successful(1)

      when(
        consignmentStatusRepositoryMock.updateConsignmentStatus(
          consignmentIdCaptor.capture(),
          statusTypeCaptor.capture(),
          statusValueCaptor.capture(),
          modifiedTimeCaptor.capture()
        )
      ).thenReturn(consignmentStatusMockRepoResponse)

      val updateConsignmentStatusInput =
        ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

      val response: Int = consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue

      response should be(1)
      consignmentIdCaptor.getValue should equal(expectedConsignmentId)
      statusTypeCaptor.getValue should equal(expectedStatusType)
      statusValueCaptor.getValue should equal(expectedStatusValue)
      modifiedTimeCaptor.getValue should equal(modifiedTime)
    }
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusType has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "Completed"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(
      s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'"
    )
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusValue has been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "Series"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(
      s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'"
    )
  }

  "updateConsignmentStatus" should "throw an exception if an incorrect statusType and statusValue have been passed" in {
    val fixedUUIDSource = new FixedUUIDSource()
    val expectedConsignmentId = fixedUUIDSource.uuid
    val expectedStatusType = "InvalidStatusType"
    val expectedStatusValue = "InvalidStatusValue"

    val updateConsignmentStatusInput =
      ConsignmentStatusInput(expectedConsignmentId, expectedStatusType, Some(expectedStatusValue))

    val thrownException = intercept[Exception] {
      consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
    }

    thrownException.getMessage should equal(
      s"Invalid ConsignmentStatus input: either '$expectedStatusType' or '$expectedStatusValue'"
    )
  }

  forAll(statusTypes.filter(_ != "Upload")) { nonUploadStatusType =>
    "updateConsignmentStatus" should s"not call fileStatus repo if a non-'Upload' statusType of $nonUploadStatusType has been passed in" in {
      val fixedUUIDSource = new FixedUUIDSource()
      val expectedConsignmentId = fixedUUIDSource.uuid
      val expectedStatusValue = "Completed"
      val modifiedTime = Timestamp.from(FixedTimeSource.now)
      val consignmentIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
      val statusTypeCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val statusValueCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val modifiedTimeCaptor: ArgumentCaptor[Timestamp] = ArgumentCaptor.forClass(classOf[Timestamp])

      val consignmentStatusMockRepoResponse: Future[Int] = Future.successful(1)

      when(
        consignmentStatusRepositoryMock.updateConsignmentStatus(
          consignmentIdCaptor.capture(),
          statusTypeCaptor.capture(),
          statusValueCaptor.capture(),
          modifiedTimeCaptor.capture()
        )
      ).thenReturn(consignmentStatusMockRepoResponse)

      val updateConsignmentStatusInput =
        ConsignmentStatusInput(expectedConsignmentId, nonUploadStatusType, Some(expectedStatusValue))

      val response: Int = consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue

      response should be(1)
      consignmentIdCaptor.getValue should equal(expectedConsignmentId)
      statusTypeCaptor.getValue should equal(nonUploadStatusType)
      statusValueCaptor.getValue should equal(expectedStatusValue)
      modifiedTimeCaptor.getValue should equal(modifiedTime)
    }
  }

  forAll(statusTypes.filter(_ != "Upload")) { nonUploadStatusType =>
    "updateConsignmentStatus" should s"throw an InputDataException when a consignmentId and a non-'Upload' statusType of $nonUploadStatusType has been passed in" in {
      val fixedUUIDSource = new FixedUUIDSource()
      val expectedConsignmentId = fixedUUIDSource.uuid

      val updateConsignmentStatusInput =
        ConsignmentStatusInput(expectedConsignmentId, nonUploadStatusType, None)

      val thrownException = intercept[Exception] {
        consignmentService.updateConsignmentStatus(updateConsignmentStatusInput).futureValue
      }

      thrownException.getMessage should equal(
        s"Invalid ConsignmentStatus input: either '$nonUploadStatusType' or ''"
      )
    }
  }

  "validStatusTypes" should "contain the correct values" in {
    val expectedValues = List("ClientChecks", "ConfirmTransfer", "Export", "Series", "ServerAntivirus", "ServerChecksum", "ServerFFID", "TransferAgreement", "Upload")
    validStatusTypes.toList.sorted should equal(expectedValues)
  }

  "validStatusValues" should "contain the correct values" in {
    val expectedValues = List("Completed", "CompletedWithIssues", "Failed", "InProgress")
    validStatusValues.toList.sorted should equal(expectedValues)
  }

  private def generateConsignmentStatusRow(
      consignmentId: UUID,
      statusType: String,
      statusValue: String,
      modifiedTime: Option[Timestamp] = Option(Timestamp.from(FixedTimeSource.now))
  ): ConsignmentstatusRow = {
    ConsignmentstatusRow(
      UUID.randomUUID(),
      consignmentId,
      statusType,
      statusValue,
      Timestamp.from(FixedTimeSource.now),
      modifiedTime
    )
  }
}
