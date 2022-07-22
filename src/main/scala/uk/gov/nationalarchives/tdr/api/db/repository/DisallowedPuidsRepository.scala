package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Disallowedpuids, DisallowedpuidsRow}

import scala.concurrent.Future

class DisallowedPuidsRepository(db: Database) {

  def getDisallowedPuid(puid: String): Future[Option[DisallowedpuidsRow]] = {
    val query = Disallowedpuids.filter(_.puid === puid)
    db.run(query.result.headOption)
  }

  def activeReasons(): Future[Seq[String]] = {
    val query = Disallowedpuids.filter(_.active === true).map(_.reason).distinct
    db.run(query.result)
  }
}
