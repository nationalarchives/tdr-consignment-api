package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.{JdbcBackend, RowsPerStatement}
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Avmetadata, AvmetadataRow, File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  private val insertFileStatusQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def addAntivirusMetadata(antivirusMetadataRow: List[AvmetadataRow]): Future[List[AvmetadataRow]] = {
    // All values are supplied by the caller, so no generated values need to be returned
    val inserts = BatchInsert.chunked(antivirusMetadataRow, Avmetadata.baseTableRow.create_*.size)(batch => Avmetadata.insertAll(batch, RowsPerStatement.All))
    db.run(DBIO.seq(inserts: _*).transactionally).map(_ => antivirusMetadataRow)
  }

  def getAntivirusMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Seq[AvmetadataRow]] = {
    val query = Avmetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filterOpt(selectedFileIds)(_._2.fileid inSetBind _)
      .map(_._1)
    db.run(query.result)
  }
}
