package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.{JdbcBackend, RowsPerStatement}
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Ffidmetadatamatches, FfidmetadatamatchesRow}

import scala.concurrent.Future

class FFIDMetadataMatchesRepository(db: JdbcBackend#Database) {
  // All values are supplied by the caller, so no generated values need to be returned
  def addFFIDMetadataMatches(rows: List[FfidmetadatamatchesRow]): Future[Seq[FfidmetadatamatchesRow]] = {
    val inserts = BatchInsert.chunked(rows, Ffidmetadatamatches.baseTableRow.create_*.size)(batch => Ffidmetadatamatches.insertAll(batch, RowsPerStatement.All))
    db.run(DBIO.seq(inserts: _*).andThen(DBIO.successful(rows)).transactionally)
  }
}
