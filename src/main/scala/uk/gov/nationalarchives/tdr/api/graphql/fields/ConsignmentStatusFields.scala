package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.macros.derive.deriveInputObjectType
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import io.circe.generic.auto._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, IntType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import java.time.ZonedDateTime
import java.util.UUID

object ConsignmentStatusFields {

  case class ConsignmentStatus(consignmentStatusId: UUID,
                               consignmentId: UUID,
                               statusType: String,
                               value: String,
                               createdDatetime: ZonedDateTime,
                               modifiedDatetime: ZonedDateTime
                              )

  case class UpdateConsignmentStatusInput(consignmentId: UUID, statusType: String, statusValue: String) extends UserOwnsConsignment

  val UpdateConsignmentStatusType: InputObjectType[UpdateConsignmentStatusInput] =
    deriveInputObjectType[UpdateConsignmentStatusInput]()

  val UploadCompleteUpdateArg: Argument[UpdateConsignmentStatusInput] = Argument("uploadCompleteUpdate", UpdateConsignmentStatusType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("updateConsignmentStatusUploadComplete", OptionType(IntType),
      arguments = UploadCompleteUpdateArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentStatusService.updateConsignmentStatusUploadComplete(ctx.arg(UploadCompleteUpdateArg)),
      tags = List(ValidateUserHasAccessToConsignment(UploadCompleteUpdateArg))
    )
  )
}
