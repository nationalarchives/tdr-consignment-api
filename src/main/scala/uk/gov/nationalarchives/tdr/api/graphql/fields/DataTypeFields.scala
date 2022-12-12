package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.macros.derive.deriveEnumType
import sangria.schema.EnumType

trait DataTypeFields {
  sealed trait DataType

  case object Text extends DataType

  case object Integer extends DataType

  case object DateTime extends DataType

  case object Decimal extends DataType

  case object Boolean extends DataType

  implicit val DataTypeType: EnumType[DataType] = deriveEnumType[DataType]()

  def toDataType(dataType: Option[String]): DataType = {
    dataType match {
      case Some("text") => Text
      case Some("datetime") => DateTime
      case Some("integer") => Integer
      case Some("decimal") => Decimal
      case Some("boolean") => Boolean
      case _ => throw new Exception(s"Invalid data type $dataType")
    }
  }
}
