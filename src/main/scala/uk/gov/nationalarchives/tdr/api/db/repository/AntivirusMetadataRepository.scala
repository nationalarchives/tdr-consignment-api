package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Avmetadata, AvmetadataRow, File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {
  private val insertAvMetadataQuery = Avmetadata returning Avmetadata.map(_.fileid) into
    ((antivirusMetadata, fileid) => antivirusMetadata.copy(fileid = fileid))

  private val insertFileStatusQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def addAntivirusMetadata(antivirusMetadataRow: AvmetadataRow, fileStatusRow: FilestatusRow): Future[AvmetadataRow] = {
    val allUpdates = DBIO.seq(insertAvMetadataQuery += antivirusMetadataRow, insertFileStatusQuery += fileStatusRow).transactionally
    db.run(allUpdates).map(_ => antivirusMetadataRow)
  }

  def getAntivirusMetadata(consignmentId: UUID): Future[Seq[AvmetadataRow]] = {
    val query = Avmetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .map(_._1)
    db.run(query.result)
  }
}
