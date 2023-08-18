package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import sangria.macros.derive._
import FieldTypes._
import sangria.marshalling.circe._
import io.circe.generic.auto._
import uk.gov.nationalarchives.tdr.api.auth.ValidateHasFFIDMetadataAccess
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext

object FFIDMetadataFields {

  case class FFIDMetadata(
      fileId: UUID,
      software: String,
      softwareVersion: String,
      binarySignatureFileVersion: String,
      containerSignatureFileVersion: String,
      method: String,
      matches: List[FFIDMetadataMatches],
      datetime: Long
  )

  case class FFIDMetadataInput(metadataInputValues: List[FFIDMetadataInputValues])

  case class FFIDMetadataInputValues(
      fileId: UUID,
      software: String,
      softwareVersion: String,
      binarySignatureFileVersion: String,
      containerSignatureFileVersion: String,
      method: String,
      matches: List[FFIDMetadataInputMatches],
      fileExtensionMismatch: Option[Boolean] = Some(false)
  )

  case class FFIDMetadataInputMatches(extension: Option[String] = None, identificationBasis: String, puid: Option[String], fileExtensionMismatch: Option[Boolean] = Some(false))
  case class FFIDMetadataMatches(extension: Option[String] = None, identificationBasis: String, puid: Option[String], fileExtensionMismatch: Option[Boolean] = Some(false))

  implicit val FFIDMetadataInputMatchesType: ObjectType[Unit, FFIDMetadataMatches] = deriveObjectType[Unit, FFIDMetadataMatches]()
  implicit val FFIDMetadataInputMatchesInputType: InputObjectType[FFIDMetadataInputMatches] = deriveInputObjectType[FFIDMetadataInputMatches]()
  implicit val AddFFFIDMetadataInputType: InputObjectType[FFIDMetadataInput] = deriveInputObjectType[FFIDMetadataInput]()
  implicit val AddFFFIDMetadataInputValuesType: InputObjectType[FFIDMetadataInputValues] = deriveInputObjectType[FFIDMetadataInputValues]()
  implicit val FFIDMetadataType: ObjectType[Unit, FFIDMetadata] = deriveObjectType[Unit, FFIDMetadata]()

  val FileFormatMetadataInputArg: Argument[FFIDMetadataInput] = Argument("addBulkFFIDMetadataInput", AddFFFIDMetadataInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addBulkFFIDMetadata",
      ListType(FFIDMetadataType),
      arguments = FileFormatMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.ffidMetadataService.addFFIDMetadata(ctx.arg(FileFormatMetadataInputArg)),
      tags = List(ValidateHasFFIDMetadataAccess)
    )
  )
}
