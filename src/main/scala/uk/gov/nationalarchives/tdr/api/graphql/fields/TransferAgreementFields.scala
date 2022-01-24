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

  case class TransferAgreementNotCompliance(consignmentId: UUID,
                                            allPublicRecords: Boolean,
                                            allCrownCopyright: Boolean,
                                            allEnglish: Boolean)

  case class TransferAgreementCompliance(consignmentId: UUID,
                                         appraisalSelectionSignedOff: Boolean,
                                         initialOpenRecords: Boolean,
                                         sensitivityReviewSignedOff: Boolean)

  case class AddTransferAgreementNotComplianceInput(consignmentId: UUID,
                                                    allPublicRecords: Boolean,
                                                    allCrownCopyright: Boolean,
                                                    allEnglish: Boolean) extends UserOwnsConsignment

  case class AddTransferAgreementComplianceInput(consignmentId: UUID,
                                                 appraisalSelectionSignedOff: Boolean,
                                                 initialOpenRecords: Boolean,
                                                 sensitivityReviewSignedOff: Boolean) extends UserOwnsConsignment

  val TransferAgreementNotComplianceType: ObjectType[Unit, TransferAgreementNotCompliance] = deriveObjectType[Unit, TransferAgreementNotCompliance]()
  val TransferAgreementComplianceType: ObjectType[Unit, TransferAgreementCompliance] = deriveObjectType[Unit, TransferAgreementCompliance]()

  val AddTransferAgreementNotComplianceInputType: InputObjectType[AddTransferAgreementNotComplianceInput] =
    deriveInputObjectType[AddTransferAgreementNotComplianceInput]()
  val AddTransferAgreementComplianceInputType: InputObjectType[AddTransferAgreementComplianceInput] =
    deriveInputObjectType[AddTransferAgreementComplianceInput]()

  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val TransferAgreementType: ObjectType[Unit, TransferAgreement] = deriveObjectType[Unit, TransferAgreement]()
  val AddTransferAgreementInputType: InputObjectType[AddTransferAgreementInput] = deriveInputObjectType[AddTransferAgreementInput]()

  val TransferAgreementInputArg: Argument[AddTransferAgreementInput] = Argument("addTransferAgreementInput", AddTransferAgreementInputType)
  val TransferAgreementNotComplianceInputArg: Argument[AddTransferAgreementNotComplianceInput] =
    Argument("addTransferAgreementNotComplianceInput", AddTransferAgreementNotComplianceInputType)
  val TransferAgreementComplianceInputArg: Argument[AddTransferAgreementComplianceInput] =
    Argument("addTransferAgreementComplianceInput", AddTransferAgreementComplianceInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addTransferAgreement", TransferAgreementType,
      arguments=TransferAgreementInputArg :: Nil,
      resolve = ctx => ctx.ctx.transferAgreementService.addTransferAgreement(ctx.arg(TransferAgreementInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(TransferAgreementInputArg))
    ),
    Field("addTransferAgreementNotCompliance", TransferAgreementNotComplianceType,
      arguments=TransferAgreementNotComplianceInputArg :: Nil,
      resolve = ctx =>
        ctx.ctx.transferAgreementService.addTransferAgreementNotCompliance(ctx.arg(TransferAgreementNotComplianceInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(TransferAgreementNotComplianceInputArg))
    ),
    Field("addTransferAgreementCompliance", TransferAgreementComplianceType,
      arguments=TransferAgreementComplianceInputArg :: Nil,
      resolve = ctx =>
        ctx.ctx.transferAgreementService.addTransferAgreementCompliance(ctx.arg(TransferAgreementComplianceInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(TransferAgreementComplianceInputArg))
    )
  )
}
