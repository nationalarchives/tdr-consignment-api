package uk.gov.nationalarchives.tdr.api.service

import slick.jdbc.JdbcBackend

import uk.gov.nationalarchives.tdr.api.db.repository.TransferringBodyRepository

import scala.concurrent.{ExecutionContext, Future}

class FullHealthCheckService()(implicit executionContext: ExecutionContext) {

  def checkDbIsUpAndRunning(db: JdbcBackend#Database): Future[Unit] = {
    val transferringBodyRepository = new TransferringBodyRepository(db)
    transferringBodyRepository.dbHasTransferringBodies.map { bodiesInDb =>
      if (bodiesInDb) () else throw new IllegalStateException("Health Check failed because there are no Transferring Bodies in the DB.")
    }
  }
}
