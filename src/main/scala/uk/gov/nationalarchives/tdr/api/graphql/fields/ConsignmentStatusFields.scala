package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.deriveInputObjectType
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, IntType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import java.time.ZonedDateTime
import java.util.UUID

object ConsignmentStatusFields {

  case class UpdateConsignmentStatusInput(consignmentId: UUID,
                                          statusType: String,
                                          statusValue: String) extends UserOwnsConsignment

  val UpdateConsignmentStatusInputType: InputObjectType[UpdateConsignmentStatusInput] =
    deriveInputObjectType[UpdateConsignmentStatusInput]()

  val UpdateConsignmentStatusArg: Argument[UpdateConsignmentStatusInput] =
    Argument("updateConsignmentStatus", UpdateConsignmentStatusInputType)
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("markUploadAsCompleted", OptionType(IntType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.updateConsignmentStatus(
        ctx.arg(ConsignmentIdArg),
        "Upload",
        "Completed"
      ),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field("updateConsignmentStatus", OptionType(IntType),
      arguments = UpdateConsignmentStatusArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.updateConsignmentStatus(
        ctx.arg(UpdateConsignmentStatusArg)
      ),
      tags = List(ValidateUserHasAccessToConsignment(UpdateConsignmentStatusArg))
    )
  )

  case class ConsignmentStatus(consignmentStatusId: UUID,
                               consignmentId: UUID,
                               statusType: String,
                               value: String,
                               createdDatetime: ZonedDateTime,
                               modifiedDatetime: ZonedDateTime
                              )
}
