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

  case class DisplayAttribute(attribute: String, value: String, `type`: DataType)

  case class DisplayProperty(propertyName: String, attributes: Set[DisplayAttribute])
}
