package uk.gov.nationalarchives.tdr.api.service

import java.util.UUID

import uk.gov.nationalarchives.tdr.api.db.repository.TransferAgreementRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.{AddTransferAgreementInput, TransferAgreement}
import uk.gov.nationalarchives.Tables.TransferagreementRow

import scala.concurrent.{ExecutionContext, Future}

class TransferAgreementService(transferAgreementRepository: TransferAgreementRepository, uuidSource: UUIDSource)
                              (implicit val executionContext: ExecutionContext) {

  def addTransferAgreement(input: AddTransferAgreementInput): Future[TransferAgreement] = {
    val transferAgreementRow = TransferagreementRow(uuidSource.uuid, input.consignmentId,
      input.allPublicRecords,
      input.allCrownCopyright,
      input.allEnglish,
      //temporarily add default allDigital value to decouple from frontend input until column dropped from Db
      //required as otherwise Db row is not filled in correctly
      Some(true),
      input.appraisalSelectionSignedOff,
      input.sensitivityReviewSignedOff)

    transferAgreementRepository.addTransferAgreement(transferAgreementRow).map(dbRowToTransferAgreement)
  }

  def getTransferAgreement(consignmentId: UUID): Future[Option[TransferAgreement]] = {
    transferAgreementRepository.getTransferAgreement(consignmentId)
      .map(ta => ta.headOption.map(dbRowToTransferAgreement))
  }

  private def agreed(field: Option[Boolean]) = field.isDefined && field.get

  private def isAgreementComplete(ta: TransferagreementRow): Boolean = {
    val fields = List(ta.allcrowncopyright, ta.alldigital, ta.allenglish, ta.allpublicrecords, ta.appraisalselectionsignedoff, ta.sensitivityreviewsignedoff)
    fields.forall(agreed)
  }

  private def dbRowToTransferAgreement(row: TransferagreementRow ): TransferAgreement = {
    TransferAgreement(row.consignmentid,
      row.allpublicrecords,
      row.allcrowncopyright,
      row.allenglish,
      row.alldigital,
      row.appraisalselectionsignedoff,
      row.sensitivityreviewsignedoff,
      Some(row.transferagreementid),
      isAgreementComplete(row)
    )
  }
}
