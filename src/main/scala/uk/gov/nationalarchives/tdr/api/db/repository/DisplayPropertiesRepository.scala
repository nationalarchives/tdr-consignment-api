package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Displayproperties, DisplaypropertiesRow}

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  def getDisplayProperties(attribute: Option[String] = None, value: Option[String] = None): Future[Seq[DisplaypropertiesRow]] = {
    val query = Displayproperties
      .filterOpt(attribute)(_.attribute === _)
      .filterOpt(value)(_.value === _)
    db.run(query.result)
  }
}
