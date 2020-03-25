package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.marshalling.circe._
import io.circe.generic.auto._
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import sangria.macros.derive._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, OptionInputType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.graphql.Tags.{ValidateBody, ValidateIsAdmin}

object SeriesFields {
  case class Series(seriesid: Long, bodyid: Long, name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)
  case class AddSeriesInput(bodyid: Long, name: Option[String] = None, code: Option[String] = None, description: Option[String] = None)

  implicit val SeriesType: ObjectType[Unit, Series] = deriveObjectType[Unit, Series]()
  implicit val AddSeriesInputType: InputObjectType[AddSeriesInput] = deriveInputObjectType[AddSeriesInput]()

  val BodyArg = Argument("body", StringType)

  private val SeriesInputArg = Argument("addSeriesInput", AddSeriesInputType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getSeries", ListType(SeriesType),
      arguments=BodyArg :: Nil,
      resolve = ctx => ctx.ctx.seriesService.getSeries(ctx.arg(BodyArg)),
      tags=List(ValidateBody()))
  )

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addSeries",
      SeriesType,
      arguments = List(SeriesInputArg),
      resolve = ctx => ctx.ctx.seriesService.addSeries(ctx.arg(SeriesInputArg)),
      tags=List(ValidateIsAdmin())
    )
    )
}
