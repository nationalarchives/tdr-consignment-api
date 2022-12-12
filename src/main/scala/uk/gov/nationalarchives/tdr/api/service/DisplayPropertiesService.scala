package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.tdr.api.db.repository.DisplayPropertiesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.DisplayPropertiesFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.DisplayPropertiesFields

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesService(displayPropertiesRepository: DisplayPropertiesRepository)(implicit val ec: ExecutionContext) {

  implicit class DisplayPropertyRowHelper(properties: Seq[Tables.DisplaypropertiesRow]) {
    def toDisplayProperty: Seq[DisplayProperty] = {
      properties.groupBy(_.propertyname).map(r => {
        val name = r._1.get
        val attributes = r._2.map(a => {
          DisplayAttribute(a.attribute.getOrElse(""), a.value.getOrElse(""), DisplayPropertiesFields.toDataType(a.attributetype))
        }).toSet
        DisplayProperty(name, attributes)
      }).toSeq
    }
  }

  def getDisplayProperties: Future[Seq[DisplayProperty]] = {
    for {
      displayProperties <- displayPropertiesRepository.getDisplayProperties
    } yield {
      displayProperties.toDisplayProperty
    }
  }
}
