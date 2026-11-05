package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.{JdbcBackend, RowsPerStatement}
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Ffidmetadata, FfidmetadataRow, Ffidmetadatamatches, FfidmetadatamatchesRow, File}
import uk.gov.nationalarchives.tdr.api.db.repository.FFIDMetadataRepository.FFIDRepositoryMetadata

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {

  // All values, including ffidmetadataid, are supplied by the caller, so no generated values need to be returned
  def addFFIDMetadata(ffidMetadataRows: List[FfidmetadataRow]): Future[List[FfidmetadataRow]] = {
    val inserts = BatchInsert.chunked(ffidMetadataRows, Ffidmetadata.baseTableRow.create_*.size)(batch => Ffidmetadata.insertAll(batch, RowsPerStatement.All))
    db.run(DBIO.seq(inserts: _*).transactionally).map(_ => ffidMetadataRows)
  }

  def getFFIDMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Seq[FFIDRepositoryMetadata]] = {
    val query = Ffidmetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .join(Ffidmetadatamatches)
      .on(_._1.ffidmetadataid === _.ffidmetadataid)
      .filter(_._1._2.consignmentid === consignmentId)
      .filterOpt(selectedFileIds)(_._1._2.fileid inSetBind _)
      .map(res => (res._1._1, res._2))
    db.run(query.result)
  }
}

object FFIDMetadataRepository {
  type FFIDRepositoryMetadata = (FfidmetadataRow, FfidmetadatamatchesRow)
}
