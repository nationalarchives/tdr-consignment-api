package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.Disallowedpuids

import scala.concurrent.Future

class DisallowedPuidsRepository(db: Database) {

  def getDisallowedPuidReason(puid: String): Future[Option[String]] = {
    val query = Disallowedpuids.filter(_.puid === puid).map(_.reason)
    db.run(query.result.headOption)
  }
}
