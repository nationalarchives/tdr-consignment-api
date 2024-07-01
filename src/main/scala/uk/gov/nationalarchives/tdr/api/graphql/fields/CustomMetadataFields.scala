package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.marshalling.circe._
import sangria.macros.derive.{deriveEnumType, deriveObjectType}
import sangria.schema.{Argument, EnumType, Field, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.fields.DataTypeFields

import java.util.UUID

object CustomMetadataFields extends DataTypeFields {

  sealed trait PropertyType
  case object System extends PropertyType
  case object Defined extends PropertyType
  case object Supplied extends PropertyType

  case class CustomMetadataValues(dependencies: List[CustomMetadataField], value: String, uiOrdinal: Int)
  case class CustomMetadataField(
      name: String,
      fullName: Option[String],
      description: Option[String],
      propertyType: PropertyType,
      propertyGroup: Option[String],
      dataType: DataType,
      editable: Boolean,
      multiValue: Boolean,
      defaultValue: Option[String],
      values: List[CustomMetadataValues],
      uiOrdinal: Int,
      allowExport: Boolean = false,
      exportOrdinal: Option[Int] = None,
      propertyProtected: Boolean
  )

  implicit val PropertyTypeType: EnumType[PropertyType] = deriveEnumType[PropertyType]()
  implicit val MetadataFieldsType: ObjectType[Unit, CustomMetadataField] = deriveObjectType[Unit, CustomMetadataField]()
  implicit val MetadataValuesType: ObjectType[Unit, CustomMetadataValues] = deriveObjectType[Unit, CustomMetadataValues]()
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "customMetadata",
      ListType(MetadataFieldsType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.customMetadataPropertiesService.getCustomMetadata,
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    )
  )
}
