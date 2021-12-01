package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.macros.derive.{deriveEnumType, deriveObjectType}
import sangria.schema.{Argument, EnumType, Field, ListType, ObjectType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateNotHavingToUseAToken
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

import java.util.UUID

object MetadataFields {
  sealed trait DataType
  case object Text extends DataType
  case object Integer extends DataType
  case object DateTime extends DataType
  case object Decimal extends DataType

  sealed trait PropertyType
  case object System extends PropertyType
  case object Defined extends PropertyType
  case object Supplied extends PropertyType


  case class MetadataValues(dependencies: List[MetadataField], value: String)
  case class MetadataField(
                       name: String, fullName: Option[String], description: Option[String], propertyType: PropertyType,
                       propertyGroup: Option[String], dataType: DataType, editable: Boolean,
                       multiValue: Boolean, defaultValue: Option[String], values: List[MetadataValues]
                     )

  implicit val DataTypeType: EnumType[DataType] = deriveEnumType[DataType]()
  implicit val PropertyTypeType: EnumType[PropertyType] = deriveEnumType[PropertyType]()
  implicit val MetadataFieldsType: ObjectType[Unit, MetadataField] = deriveObjectType[Unit, MetadataField]()
  implicit val MetadataValuesType: ObjectType[Unit, MetadataValues] = deriveObjectType[Unit, MetadataValues]()

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("metadata", ListType(MetadataFieldsType),
      arguments=Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.getCustomMetadata,
      tags=List(ValidateNotHavingToUseAToken())
    )
  )


}
