package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields._

import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesService(customMetadataPropertiesRepository: CustomMetadataPropertiesRepository)
                                      (implicit val ec: ExecutionContext) {

  def getClosureMetadata: Future[Seq[CustomMetadataField]] = {
    (for {
      properties <- customMetadataPropertiesRepository.getClosureMetadataProperty
      values <- customMetadataPropertiesRepository.getClosureMetadataValues
      dependencies <- customMetadataPropertiesRepository.getClosureMetadataDependencies
    } yield (properties, values, dependencies)).map {
      case (propertiesResult, valuesResult, dependenciesResult) =>
        val values: Map[String, Seq[FilepropertyvaluesRow]] = valuesResult.groupBy(_.propertyname)
        val dependencies: Map[Int, Seq[FilepropertydependenciesRow]] = dependenciesResult.groupBy(_.groupid)

        def rowsToMetadata(fp: FilepropertyRow, defaultValueOption: Option[String] = None): CustomMetadataField = {
          val metadataValues: Seq[CustomMetadataValues] = values.getOrElse(fp.name, Nil).map(value => {
            value.dependencies.map(groupId => {
              val deps: Seq[CustomMetadataField] = for {
                dep <- dependencies.getOrElse(groupId, Nil)
                dependencyProps <- propertiesResult.find(_.name == dep.propertyname).map(fp => {
                  rowsToMetadata(fp, dep.default)
                })
              } yield dependencyProps
              CustomMetadataValues(deps.toList, value.propertyvalue)
            }).getOrElse(CustomMetadataValues(Nil, value.propertyvalue))
          })
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
            metadataValues.toList
          )
        }

        propertiesResult.map(prop => {
          val defaultValue: Option[String] = for {
            values <- values.get(prop.name)
            value <- values.find(_.default.getOrElse(false))
          } yield value.propertyvalue
          rowsToMetadata(prop, defaultValue)
        }).toList
    }
  }

  def getPropertyType(propertyType: Option[String]): PropertyType = propertyType match {
    case Some("System") => System
    case Some("Defined") => Defined
    case Some("Supplied") => Supplied
    case _ => throw new Exception(s"Invalid property type $propertyType")
  }

  def getDataType(dataType: Option[String]): DataType = dataType match {
    case Some("text") => Text
    case Some("datetime") => DateTime
    case Some("integer") => Integer
    case Some("decimal") => Decimal
    case _ => throw new Exception(s"Invalid data type $dataType")
  }
}
