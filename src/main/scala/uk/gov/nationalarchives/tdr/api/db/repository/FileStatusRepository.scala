package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.Future

class FileStatusRepository(db: Database) {

  def getFileStatus(consignmentId: UUID, statusType: String): Future[Seq[FilestatusRow]] = {
    val query = Filestatus.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._1.statustype === statusType)
      .map(_._1)
    db.run(query.result)
  }
}
