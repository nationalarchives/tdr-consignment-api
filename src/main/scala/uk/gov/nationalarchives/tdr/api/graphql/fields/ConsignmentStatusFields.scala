package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, IntType, ObjectType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.{UuidType, ZonedDateTimeType}
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import java.time.ZonedDateTime
import java.util.UUID

object ConsignmentStatusFields {

  case class ConsignmentStatus(consignmentStatusId: UUID,
                               consignmentId: UUID,
                               statusType: String,
                               value: String,
                               createdDatetime: ZonedDateTime,
                               modifiedDatetime: Option[ZonedDateTime])

  case class ConsignmentStatusInput(consignmentId: UUID,
                                    statusType: String,
                                    statusValue: String) extends UserOwnsConsignment

  val ConsignmentStatusInputType: InputObjectType[ConsignmentStatusInput] =
    deriveInputObjectType[ConsignmentStatusInput]()
  implicit val ConsignmentStatusType: ObjectType[Unit, ConsignmentStatus] = deriveObjectType[Unit, ConsignmentStatus]()

  val AddConsignmentStatusArg: Argument[ConsignmentStatusInput] =
    Argument("addConsignmentStatusInput", ConsignmentStatusInputType)
  val UpdateConsignmentStatusArg: Argument[ConsignmentStatusInput] =
    Argument("updateConsignmentStatusInput", ConsignmentStatusInputType)
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addConsignmentStatus", ConsignmentStatusType,
      arguments = AddConsignmentStatusArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.addConsignmentStatus(ctx.arg(AddConsignmentStatusArg)),
      tags = List(ValidateUserHasAccessToConsignment(AddConsignmentStatusArg))
    ),
    Field("updateConsignmentStatus", OptionType(IntType),
      arguments = UpdateConsignmentStatusArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.updateConsignmentStatus(
        ctx.arg(UpdateConsignmentStatusArg)
      ),
      tags = List(ValidateUserHasAccessToConsignment(UpdateConsignmentStatusArg))
    )
  )
}
