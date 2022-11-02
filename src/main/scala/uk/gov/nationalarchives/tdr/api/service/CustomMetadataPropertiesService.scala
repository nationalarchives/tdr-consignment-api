package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields._

import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesService(customMetadataPropertiesRepository: CustomMetadataPropertiesRepository)(implicit val ec: ExecutionContext) {

  def getCustomMetadata: Future[Seq[CustomMetadataField]] = {
    val propertiesValuesAndDependencies: Future[(Seq[FilepropertyRow], Seq[FilepropertyvaluesRow], Seq[FilepropertydependenciesRow])] =
      for {
        properties <- customMetadataPropertiesRepository.getCustomMetadataProperty
        values <- customMetadataPropertiesRepository.getCustomMetadataValues
        dependencies <- customMetadataPropertiesRepository.getCustomMetadataDependencies
      } yield (properties, values, dependencies)

    propertiesValuesAndDependencies.map { case (properties, values, dependencies) =>
      val propertyNames: Seq[String] = properties.map(_.name)
      val valuesByPropertyName: Map[String, Seq[FilepropertyvaluesRow]] = values.groupBy(_.propertyname)
      val propertyNamesValuesBelongTo: Seq[String] = valuesByPropertyName.keys.toSeq
      val dependencyNames: Seq[String] = dependencies.map(_.propertyname)
      checkProperties(propertyNames, Map("values" -> propertyNamesValuesBelongTo, "dependencies" -> dependencyNames))

      val dependenciesByGroupId: Map[Int, Seq[FilepropertydependenciesRow]] = dependencies.groupBy(_.groupid)

      properties.map { property =>
        {
          val defaultValue: Option[String] = for {
            valuesOfProperty <- valuesByPropertyName.get(property.name)
            valueLabelledAsTheDefault <- valuesOfProperty.find(_.default.getOrElse(false))
          } yield valueLabelledAsTheDefault.propertyvalue
          rowsToMetadata(property, valuesByPropertyName, dependenciesByGroupId, properties, defaultValue)
        }
      }.toList
    }
  }

  def checkProperties(properties: Seq[String], valuesAndDependencies: Map[String, Seq[String]]): Unit =
    valuesAndDependencies.foreach { case (table, names) =>
      names.foreach { name =>
        if (!properties.contains(name))
          throw new Exception(
            s"Error: Property name $name, in the $table table, does not exist in the FileProperty table"
          )
      }
    }

  private def rowsToMetadata(
      fp: FilepropertyRow,
      values: Map[String, Seq[FilepropertyvaluesRow]],
      dependencies: Map[Int, Seq[FilepropertydependenciesRow]],
      properties: Seq[FilepropertyRow],
      defaultValueOption: Option[String] = None
  ): CustomMetadataField = {

    val metadataValues: Seq[CustomMetadataValues] = values.getOrElse(fp.name, Nil).map { value =>
      val valueOrdinal = value.ordinal.getOrElse(Int.MaxValue)
      value.dependencies
        .map { groupId =>
          {
            val valueDependencies: Seq[CustomMetadataField] =
              for {
                dependencyBelongingToGroupId <- dependencies.getOrElse(groupId, Nil)
                dependencyProperty <- {
                  val propertyBelongingToGroupId = properties.find(_.name == dependencyBelongingToGroupId.propertyname)
                  propertyBelongingToGroupId.map(fp => rowsToMetadata(fp, values, dependencies, properties, dependencyBelongingToGroupId.default))
                }
              } yield dependencyProperty
            CustomMetadataValues(valueDependencies.toList, value.propertyvalue, valueOrdinal)
          }
        }
        .getOrElse(CustomMetadataValues(Nil, value.propertyvalue, valueOrdinal))
    }

    CustomMetadataField(
      fp.name,
      fp.fullname,
      fp.description,
      getPropertyType(fp.propertytype),
      fp.propertygroup,
      getDataType(fp.datatype),
      fp.editable.getOrElse(false),
      fp.multivalue.getOrElse(false),
      defaultValueOption,
      metadataValues.toList,
      fp.uiordinal.getOrElse(Int.MaxValue),
      fp.allowexport,
      fp.exportordinal.map(_.toInt)
    )
  }

  private def getPropertyType(propertyType: Option[String]): PropertyType = propertyType match {
    case Some("System")   => System
    case Some("Defined")  => Defined
    case Some("Supplied") => Supplied
    case _                => throw new Exception(s"Invalid property type $propertyType")
  }

  private def getDataType(dataType: Option[String]): DataType = dataType match {
    case Some("text")     => Text
    case Some("datetime") => DateTime
    case Some("integer")  => Integer
    case Some("decimal")  => Decimal
    case Some("boolean")  => Boolean
    case _                => throw new Exception(s"Invalid data type $dataType")
  }
}
