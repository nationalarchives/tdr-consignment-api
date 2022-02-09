package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.macros.derive.{deriveEnumType, deriveObjectType}
import sangria.schema.{EnumType, Field, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext

object CustomMetadataFields {
  sealed trait DataType
  case object Text extends DataType
  case object Integer extends DataType
  case object DateTime extends DataType
  case object Decimal extends DataType

  sealed trait PropertyType
  case object System extends PropertyType
  case object Defined extends PropertyType
  case object Supplied extends PropertyType

  case class CustomMetadataValues(dependencies: List[CustomMetadataField], value: String)
  case class CustomMetadataField(
                            name: String, fullName: Option[String], description: Option[String], propertyType: PropertyType,
                            propertyGroup: Option[String], dataType: DataType, editable: Boolean,
                            multiValue: Boolean, defaultValue: Option[String], values: List[CustomMetadataValues]
                          )

  implicit val DataTypeType: EnumType[DataType] = deriveEnumType[DataType]()
  implicit val PropertyTypeType: EnumType[PropertyType] = deriveEnumType[PropertyType]()
  implicit val MetadataFieldsType: ObjectType[Unit, CustomMetadataField] = deriveObjectType[Unit, CustomMetadataField]()
  implicit val MetadataValuesType: ObjectType[Unit, CustomMetadataValues] = deriveObjectType[Unit, CustomMetadataValues]()

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getClosureMetadata", ListType(MetadataFieldsType),
      arguments = Nil,
      resolve = ctx => ctx.ctx.customMetadataPropertiesService.getClosureMetadata,
      tags=List()
    )
  )
}
