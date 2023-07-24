package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.{deriveEnumType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, EnumType, Field, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType

import java.util.UUID

object DisplayPropertiesFields extends DataTypeFields {

  case class DisplayAttribute(attribute: String, value: Option[String], `type`: DataType)

  case class DisplayPropertyField(propertyName: String, attributes: Seq[DisplayAttribute])

  implicit val DisplayPropertyFieldType: ObjectType[Unit, DisplayPropertyField] = deriveObjectType[Unit, DisplayPropertyField]()
  implicit val DisplayAttributeType: ObjectType[Unit, DisplayAttribute] = deriveObjectType[Unit, DisplayAttribute]()
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "displayProperties",
      ListType(DisplayPropertyFieldType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.displayPropertiesService.getDisplayProperties,
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    )
  )
}
