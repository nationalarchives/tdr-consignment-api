package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, ListType, ObjectType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.Tags.ValidateBody

object SeriesFields {
  case class Series(seriesid: Long, bodyid: Long, name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)

  implicit val SeriesType: ObjectType[Unit, Series] = deriveObjectType[Unit, Series]()

  val BodyArg = Argument("body", StringType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getSeries", ListType(SeriesType),
      arguments=BodyArg :: Nil,
      resolve = ctx => ctx.ctx.seriesService.getSeries(ctx.arg(BodyArg)),
      tags=List(ValidateBody()))
  )
}
