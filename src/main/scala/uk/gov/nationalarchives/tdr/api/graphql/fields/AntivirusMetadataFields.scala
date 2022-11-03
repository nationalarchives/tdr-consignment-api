package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListInputType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateHasAntiVirusMetadataAccess
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

object AntivirusMetadataFields {
  case class AntivirusMetadata(fileId: UUID, software: String, softwareVersion: String, databaseVersion: String, result: String, datetime: Long)

  case class AddAntivirusMetadataInput(fileId: UUID, software: String, softwareVersion: String, databaseVersion: String, result: String, datetime: Long)

  implicit val AntivirusMetadataType: ObjectType[Unit, AntivirusMetadata] = deriveObjectType[Unit, AntivirusMetadata]()
  implicit val AddAntivirusMetadataInputType: InputObjectType[AddAntivirusMetadataInput] = deriveInputObjectType[AddAntivirusMetadataInput]()

  val AntivirusMetadataInputArg = Argument("addAntivirusMetadataInput", AddAntivirusMetadataInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addAntivirusMetadata",
      AntivirusMetadataType,
      arguments = AntivirusMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.antivirusMetadataService.addAntivirusMetadata(ctx.arg(AntivirusMetadataInputArg)),
      tags = List(ValidateHasAntiVirusMetadataAccess)
    )
  )
}
