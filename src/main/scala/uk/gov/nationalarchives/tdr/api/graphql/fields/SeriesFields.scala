package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, ListInputType, ListType, ObjectType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateBody
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

import java.util.UUID

object SeriesFields {
  case class Series(seriesid: UUID, bodyid: UUID, name: String, code: String, description: Option[String] = None)

  implicit val SeriesType: ObjectType[Unit, Series] = deriveObjectType[Unit, Series]()

  val BodiesArg = Argument("bodies", ListInputType(StringType))

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "getSeries",
      ListType(SeriesType),
      arguments = BodiesArg :: Nil,
      resolve = ctx => ctx.ctx.seriesService.getSeries(ctx.arg(BodiesArg)),
      tags = List(ValidateBody)
    )
  )
}
