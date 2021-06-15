package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, IntType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType

import java.time.ZonedDateTime
import java.util.UUID

object ConsignmentStatusFields {

  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("markUploadAsCompleted", OptionType(IntType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.setUploadConsignmentStatusValueToComplete(ctx.arg(ConsignmentIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
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
