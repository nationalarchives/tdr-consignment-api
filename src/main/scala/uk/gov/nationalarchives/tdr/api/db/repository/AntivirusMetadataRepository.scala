package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Avmetadata, AvmetadataRow, File}

import java.util.UUID
import scala.concurrent.Future

class AntivirusMetadataRepository(db: Database) {
  private val insertQuery = Avmetadata returning Avmetadata.map(_.fileid) into
    ((antivirusMetadata, fileid) => antivirusMetadata.copy(fileid = fileid))

  def addAntivirusMetadata(antivirusMetadataRow: AvmetadataRow): Future[AvmetadataRow] = {
    db.run(insertQuery += antivirusMetadataRow)
  }

  def getAntivirusMetadata(consignmentId: UUID): Future[Seq[AvmetadataRow]] = {
    val query = Avmetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .map(_._1)
    db.run(query.result)
  }
}
