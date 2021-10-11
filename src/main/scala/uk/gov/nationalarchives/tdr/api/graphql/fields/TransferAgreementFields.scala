package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, LongType, ObjectType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

object TransferAgreementFields {

  case class TransferAgreement(consignmentId: UUID,
                               allPublicRecords: Boolean,
                               allCrownCopyright: Boolean,
                               allEnglish: Boolean,
                               appraisalSelectionSignedOff: Boolean,
                               initialOpenRecords: Boolean,
                               sensitivityReviewSignedOff: Boolean)

  case class AddTransferAgreementInput(consignmentId: UUID,
                                       allPublicRecords: Boolean,
                                       allCrownCopyright: Boolean,
                                       allEnglish: Boolean,
                                       appraisalSelectionSignedOff: Boolean,
                                       initialOpenRecords: Boolean,
                                       sensitivityReviewSignedOff: Boolean) extends UserOwnsConsignment

  implicit val TransferAgreementType: ObjectType[Unit, TransferAgreement] = deriveObjectType[Unit, TransferAgreement]()
  implicit val AddTransferAgreementInputType: InputObjectType[AddTransferAgreementInput] = deriveInputObjectType[AddTransferAgreementInput]()

  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val TransferAgreementInputArg: Argument[AddTransferAgreementInput] = Argument("addTransferAgreementInput", AddTransferAgreementInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addTransferAgreement", TransferAgreementType,
      arguments=TransferAgreementInputArg :: Nil,
      resolve = ctx => ctx.ctx.transferAgreementService.addTransferAgreement(ctx.arg(TransferAgreementInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(TransferAgreementInputArg))
    ))

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getTransferAgreement", OptionType(TransferAgreementType),
      arguments=ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.transferAgreementService.getTransferAgreement(ctx.arg(ConsignmentIdArg)),
      tags=List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ))
}
