package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Ffidmetadatamatches, FfidmetadatamatchesRow}

import scala.concurrent.Future

class FFIDMetadataMatchesRepository(db: Database) {
  private val insertQuery = Ffidmetadatamatches returning Ffidmetadatamatches.map(_.ffidmetadataid) into
    ((ffidMetadata, ffidmetadataid) => ffidMetadata.copy(ffidmetadataid = ffidmetadataid))

  def addFFIDMetadataMatches(rows: List[FfidmetadatamatchesRow]): Future[Seq[FfidmetadatamatchesRow]] = {
    db.run(insertQuery ++= rows)
  }
}
