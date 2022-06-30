package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Allowedpuids, Disallowedpuids}

import scala.concurrent.Future

class PUIDRepository(db: Database) {

  def getDisallowedPuidReason(puid: String): Future[Option[String]] = {
    val query = Disallowedpuids.filter(_.puid === puid).map(_.reason)
    db.run(query.result.headOption)
  }

  def checkAllowedPuidExists(puid: String): Future[Boolean] = {
    val query = Allowedpuids.filter(_.puid === puid).exists
    db.run(query.result)
  }
}
