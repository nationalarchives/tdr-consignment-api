package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import sangria.marshalling.circe._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, ListType, ObjectType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateBody
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext

object SeriesFields {
  case class Series(seriesid: UUID, bodyid: UUID, name: String, code: String, description: Option[String] = None)

  implicit val SeriesType: ObjectType[Unit, Series] = deriveObjectType[Unit, Series]()

  val BodyArg = Argument("body", StringType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getSeries", ListType(SeriesType),
      arguments=BodyArg :: Nil,
      resolve = ctx => ctx.ctx.seriesService.getSeries(ctx.arg(BodyArg)),
      tags=List(ValidateBody)
    )
  )
}
