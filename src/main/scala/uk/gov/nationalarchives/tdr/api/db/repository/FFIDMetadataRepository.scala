package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Ffidmetadata, FfidmetadataRow, Ffidmetadatamatches, FfidmetadatamatchesRow, File, Filestatus, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FFIDMetadataRepository.FFIDRepositoryMetadata
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  private val insertFFIDMetadataQuery = Ffidmetadata returning Ffidmetadata.map(_.fileid) into
    ((ffidMetadata, fileid) => ffidMetadata.copy(fileid = fileid))

  private val insertFileStatusQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def addFFIDMetadata(ffidMetadataRow: FfidmetadataRow, fileStatusRows: List[FilestatusRow]): Future[FfidmetadataRow] = {
    val allUpdates = DBIO.seq(insertFFIDMetadataQuery += ffidMetadataRow, insertFileStatusQuery ++= fileStatusRows).transactionally
    db.run(allUpdates).map(_ => ffidMetadataRow)
  }

  def countProcessedFfidMetadata(consignmentId: UUID): Future[Int] = {
    val query = Ffidmetadata.join(File)
      .on(_.fileid === _.fileid).join(Ffidmetadatamatches)
      .on(_._1.ffidmetadataid === _.ffidmetadataid)
      .filter(_._1._2.consignmentid === consignmentId)
      .groupBy(_._1._2.fileid)
      .map(_._1)
      .length
    db.run(query.result)
  }

  def getFFIDMetadata(consignmentId: UUID): Future[Seq[FFIDRepositoryMetadata]] = {
    val query = Ffidmetadata.join(File)
      .on(_.fileid === _.fileid).join(Ffidmetadatamatches)
      .on(_._1.ffidmetadataid === _.ffidmetadataid)
      .filter(_._1._2.consignmentid === consignmentId)
      .map(res => (res._1._1, res._2))
    db.run(query.result)
  }
}

object FFIDMetadataRepository {
  type FFIDRepositoryMetadata = (FfidmetadataRow, FfidmetadatamatchesRow)
}
