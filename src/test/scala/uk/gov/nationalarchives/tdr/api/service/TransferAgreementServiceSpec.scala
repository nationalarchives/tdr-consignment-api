package uk.gov.nationalarchives.tdr.api.service

import java.util.UUID

import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.TransferagreementRow
import uk.gov.nationalarchives.tdr.api.db.repository.TransferAgreementRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{AddTransferAgreementInput, TransferAgreement}
import uk.gov.nationalarchives.tdr.api.utils.FixedUUIDSource

import scala.concurrent.{ExecutionContext, Future}

class TransferAgreementServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global
  val fixedUuidSource = new FixedUUIDSource()

  "addTransferAgreement" should "create a transfer agreement given correct arguments" in {
    val repositoryMock = mock[TransferAgreementRepository]
    val transferAgreementUuid = UUID.randomUUID()
    val consignmentUuid = UUID.randomUUID()
    val mockResponse = Future.successful(TransferagreementRow(transferAgreementUuid,consignmentUuid,
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true)))

    when(repositoryMock.addTransferAgreement(any[TransferagreementRow])).thenReturn(mockResponse)
    val service = new TransferAgreementService(repositoryMock, fixedUuidSource)
    val result: TransferAgreement = service.addTransferAgreement(AddTransferAgreementInput(consignmentUuid,
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true))).futureValue

    result.consignmentId shouldBe consignmentUuid
    result.allCrownCopyright.get shouldBe true
    result.allEnglish.get shouldBe true
    result.allPublicRecords.get shouldBe true
    result.appraisalSelectionSignedOff.get shouldBe true
    result.sensitivityReviewSignedOff.get shouldBe true
  }
}
