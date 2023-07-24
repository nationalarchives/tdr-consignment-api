package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Ffidmetadata, FfidmetadataRow, Ffidmetadatamatches, FfidmetadatamatchesRow, File}
import uk.gov.nationalarchives.tdr.api.db.repository.FFIDMetadataRepository.FFIDRepositoryMetadata
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  private val insertFFIDMetadataQuery = Ffidmetadata returning Ffidmetadata.map(_.fileid) into
    ((ffidMetadata, fileid) => ffidMetadata.copy(fileid = fileid))

  def addFFIDMetadata(ffidMetadataRows: List[FfidmetadataRow]): Future[List[FfidmetadataRow]] = {
    val update = insertFFIDMetadataQuery ++= ffidMetadataRows
    db.run(update).map(_ => ffidMetadataRows)
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
