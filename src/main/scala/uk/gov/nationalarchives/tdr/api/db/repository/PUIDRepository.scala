package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Allowedpuids, Disallowedpuids, _}

import scala.concurrent.Future

class PUIDRepository(db: Database) {

  def getAllowedPUIDs: Future[Seq[AllowedpuidsRow]] = {
    db.run(Allowedpuids.result)
  }

  def getDisallowedPUIDs(reason: String): Future[Seq[DisallowedpuidsRow]] = {
    val query = Disallowedpuids.filter(_.reason === reason)
    db.run(query.result)
  }

  def getDisallowedPUIDs: Future[Seq[DisallowedpuidsRow]] = {
    db.run(Disallowedpuids.result)
  }

  def checkPuidExists(puid: String): Future[Option[String]] = {
    val query = Disallowedpuids.filter(_.puid === puid).take(1).map(_.reason)
    db.run(query.result.head)
  }

  def checkPuidAllowedExists(puid: String): Future[Boolean] = {
    val query = Allowedpuids.filter(_.puid === puid).exists//.take(1).map(_.puid)
    db.run(query.result)
  }

  //Filter the tables by Reason?

}
