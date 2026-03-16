package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.{UuidType, ZonedDateTimeType}

import java.time.ZonedDateTime
import java.util.UUID

object MetadataReviewFields {

  case class MetadataReviewLog(
      metadataReviewLogId: UUID,
      consignmentId: UUID,
      userId: UUID,
      action: String,
      eventTime: ZonedDateTime
  )

  implicit val MetadataReviewLogType: ObjectType[Unit, MetadataReviewLog] = deriveObjectType[Unit, MetadataReviewLog]()

  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "getMetadataReviewDetails",
      ListType(MetadataReviewLogType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.metadataReviewService.getMetadataReviewDetails(ctx.arg(ConsignmentIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    )
  )
}
