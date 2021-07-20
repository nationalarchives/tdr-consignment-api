package uk.gov.nationalarchives.tdr.api.service

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.repository.TransferringBodyRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FullHealthCheckService {

  def checkDbIsUpAndRunning(db: Database): Future[Unit] = {
    val transferringBodyRepository = new TransferringBodyRepository(db)
    transferringBodyRepository.dbHasTransferringBodies.map { bodiesInDb =>
      if (bodiesInDb) () else throw new IllegalStateException("Health Check failed because there are no Transferring Bodies in the DB.")
    }
  }
}
