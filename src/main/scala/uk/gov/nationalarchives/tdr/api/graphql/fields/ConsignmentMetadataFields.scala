package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import java.util.UUID

object ConsignmentMetadataFields {

  case class ConsignmentMetadataWithConsignmentId(consignmentId: UUID, propertyName: String, value: String)
  case class AddOrUpdateConsignmentMetadata(propertyName: String, value: String)
  case class AddOrUpdateConsignmentMetadataInput(consignmentId: UUID, consignmentMetadata: Seq[AddOrUpdateConsignmentMetadata]) extends UserOwnsConsignment

  implicit val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  implicit val InputAddOrUpdateMetadataType: InputObjectType[AddOrUpdateConsignmentMetadata] = deriveInputObjectType[AddOrUpdateConsignmentMetadata]()

  val AddOrUpdateBulkFileMetadataInputType: InputObjectType[AddOrUpdateConsignmentMetadataInput] = deriveInputObjectType[AddOrUpdateConsignmentMetadataInput]()

  implicit val AddOrUpdateConsignmentMetadataInputArg: Argument[AddOrUpdateConsignmentMetadataInput] =
    Argument("addOrUpdateConsignmentMetadataInput", AddOrUpdateBulkFileMetadataInputType)

  implicit val ConsignmentMetadataWithConsignmentIdType: ObjectType[Unit, ConsignmentMetadataWithConsignmentId] = deriveObjectType[Unit, ConsignmentMetadataWithConsignmentId]()

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addOrUpdateConsignmentMetadata",
      ListType(ConsignmentMetadataWithConsignmentIdType),
      arguments = AddOrUpdateConsignmentMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentMetadataService.addOrUpdateConsignmentMetadata(ctx.arg(AddOrUpdateConsignmentMetadataInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(AddOrUpdateConsignmentMetadataInputArg))
    )
  )
}
