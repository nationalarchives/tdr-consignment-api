package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Avmetadata, AvmetadataRow, File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  private val insertAvMetadataQuery = Avmetadata returning Avmetadata.map(_.fileid) into
    ((antivirusMetadata, fileid) => antivirusMetadata.copy(fileid = fileid))

  private val insertFileStatusQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def addAntivirusMetadata(antivirusMetadataRow: List[AvmetadataRow]): Future[List[AvmetadataRow]] = {
    val update = insertAvMetadataQuery ++= antivirusMetadataRow
    db.run(update).map(_ => antivirusMetadataRow)
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
