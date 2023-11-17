package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.tdr.api.db.repository.DisplayPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.DisplayPropertiesFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.DisplayPropertiesFields

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesService(displayPropertiesRepository: DisplayPropertiesRepository)(implicit val ec: ExecutionContext) {

  private def toDisplayProperty(properties: Seq[Tables.DisplaypropertiesRow]): Seq[DisplayPropertyField] = {
    properties
      .groupBy(_.propertyname)
      .map { case (displayName, displayProperties) =>
        val name = displayName.get
        val attributes = displayProperties.map { a =>
          {
            val attribute: String = a.attribute.getOrElse(
              throw new Exception(
                s"Error: Property name '$name' has empty attribute name"
              )
            )
            DisplayAttribute(attribute, a.value, DisplayPropertiesFields.toDataType(a.attributetype))
          }
        }
        DisplayPropertyField(name, attributes)

      }
      .toSeq
  }

  def getDisplayProperties: Future[Seq[DisplayPropertyField]] = {
    for {
      displayProperties <- displayPropertiesRepository.getDisplayProperties()
    } yield {
      toDisplayProperty(displayProperties)
    }
  }

  def getActiveDisplayPropertyNames: Future[Seq[String]] = {
    for {
      displayProperties <- displayPropertiesRepository.getDisplayProperties(Some("Active"), Some("true"))
    } yield {
      displayProperties.flatMap(_.propertyname)
    }
  }
}
