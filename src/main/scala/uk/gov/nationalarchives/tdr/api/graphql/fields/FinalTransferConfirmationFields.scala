package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

object FinalTransferConfirmationFields {

  case class FinalTransferConfirmation(consignmentId: UUID,
                                       finalOpenRecordsConfirmed: Boolean,
                                       legalOwnershipTransferConfirmed: Boolean
                                      )

  case class AddFinalTransferConfirmationInput(consignmentId: UUID,
                                               finalOpenRecordsConfirmed: Boolean,
                                               legalOwnershipTransferConfirmed: Boolean
                                              ) extends UserOwnsConsignment

  case class FinalJudgmentTransferConfirmation(consignmentId: UUID,
                                               legalCustodyTransferConfirmed: Boolean
                                              )

  case class AddFinalJudgmentTransferConfirmationInput(consignmentId: UUID,
                                                       legalCustodyTransferConfirmed: Boolean
                                                      ) extends UserOwnsConsignment

  implicit val FinalTransferConfirmationType: ObjectType[Unit, FinalTransferConfirmation] =
    deriveObjectType[Unit, FinalTransferConfirmation]()
  implicit val AddFinalTransferConfirmationInputType: InputObjectType[AddFinalTransferConfirmationInput] =
    deriveInputObjectType[AddFinalTransferConfirmationInput]()

  val FinalTransferConfirmationInputArg: Argument[AddFinalTransferConfirmationInput] =
    Argument("addFinalTransferConfirmationInput", AddFinalTransferConfirmationInputType)

  implicit val addFinalTransferConfirmationType: ObjectType[Unit, FinalJudgmentTransferConfirmation] =
    deriveObjectType[Unit, FinalJudgmentTransferConfirmation]()
  implicit val AddFinalJudgmentTransferConfirmationInputType: InputObjectType[AddFinalJudgmentTransferConfirmationInput] =
    deriveInputObjectType[AddFinalJudgmentTransferConfirmationInput]()

  val FinalJudgmentTransferConfirmationInputArg: Argument[AddFinalJudgmentTransferConfirmationInput] =
    Argument("addFinalJudgmentTransferConfirmationInput", AddFinalJudgmentTransferConfirmationInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addFinalTransferConfirmation", FinalTransferConfirmationType,
      arguments = FinalTransferConfirmationInputArg :: Nil,
      resolve = ctx => ctx.ctx.finalTransferConfirmationService.addFinalTransferConfirmation(ctx.arg(FinalTransferConfirmationInputArg),
        ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(FinalTransferConfirmationInputArg))
    ),
    Field("addFinalJudgmentTransferConfirmation", addFinalTransferConfirmationType,
      arguments = FinalJudgmentTransferConfirmationInputArg :: Nil,
      resolve = ctx => ctx.ctx.finalTransferConfirmationService.addFinalJudgmentTransferConfirmation(ctx.arg(FinalJudgmentTransferConfirmationInputArg),
        ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(FinalJudgmentTransferConfirmationInputArg))
    )
  )
}
