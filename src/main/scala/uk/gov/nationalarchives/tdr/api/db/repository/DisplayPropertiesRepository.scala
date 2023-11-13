package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Displayproperties, DisplaypropertiesRow}

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  def getDisplayProperties: Future[Seq[DisplaypropertiesRow]] = {
    val query = Displayproperties
    db.run(query.result)
  }

  def getDisplayProperties(attribute: String, value: String): Future[Seq[DisplaypropertiesRow]] = {
    val query = Displayproperties
      .filter(_.attribute === attribute)
      .filter(_.value === value)
    db.run(query.result)
  }
}
