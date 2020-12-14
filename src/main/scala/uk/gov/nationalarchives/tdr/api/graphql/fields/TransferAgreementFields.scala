package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, LongType, ObjectType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

object TransferAgreementFields {

  case class TransferAgreement(consignmentId: UUID,
                               allPublicRecords: Option[Boolean] = None,
                               allCrownCopyright: Option[Boolean] = None,
                               allEnglish: Option[Boolean] = None,
                               allDigital: Option[Boolean] = None,
                               appraisalSelectionSignedOff: Option[Boolean] = None,
                               sensitivityReviewSignedOff: Option[Boolean] = None,
                               transferAgreementId: Option[UUID] = None,
                               isAgreementComplete: Boolean)

  case class AddTransferAgreementInput(consignmentId: UUID,
                                       allPublicRecords: Option[Boolean] = None,
                                       allCrownCopyright: Option[Boolean] = None,
                                       allEnglish: Option[Boolean] = None,
                                       appraisalSelectionSignedOff: Option[Boolean] = None,
                                       sensitivityReviewSignedOff: Option[Boolean] = None) extends UserOwnsConsignment

  implicit val TransferAgreementType: ObjectType[Unit, TransferAgreement] = deriveObjectType[Unit, TransferAgreement]()
  implicit val AddTransferAgreementInputType: InputObjectType[AddTransferAgreementInput] = deriveInputObjectType[AddTransferAgreementInput]()

  val ConsignmentIdArg = Argument("consignmentid", UuidType)
  val TransferAgreementInputArg = Argument("addTransferAgreementInput", AddTransferAgreementInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addTransferAgreement", TransferAgreementType,
      arguments=TransferAgreementInputArg :: Nil,
      resolve = ctx => ctx.ctx.transferAgreementService.addTransferAgreement(ctx.arg(TransferAgreementInputArg)),
      tags=List(ValidateUserOwnsConsignment(TransferAgreementInputArg))
    ))

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getTransferAgreement", OptionType(TransferAgreementType),
      arguments=ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.transferAgreementService.getTransferAgreement(ctx.arg(ConsignmentIdArg)),
      tags=List(ValidateUserOwnsConsignment(ConsignmentIdArg))
    ))
}
