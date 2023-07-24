package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Displayproperties, DisplaypropertiesRow}

import scala.concurrent.{ExecutionContext, Future}

class DisplayPropertiesRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  def getDisplayProperties: Future[Seq[DisplaypropertiesRow]] = {
    val query = Displayproperties
    db.run(query.result)
  }
}
