package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Allowedpuids, Disallowedpuids}

import scala.concurrent.Future

class AllowedPuidsRepository(db: Database) {

  def checkAllowedPuidExists(puid: String): Future[Boolean] = {
    val query = Allowedpuids.filter(_.puid === puid).exists
    db.run(query.result)
  }
}
