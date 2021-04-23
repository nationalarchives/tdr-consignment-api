package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{File, Filestatus}

import java.util.UUID
import scala.concurrent.Future

class FileStatusRepository(db: Database) {

  def getFailedFileStatuses(consignmentId: UUID): Future[Int] = {
    val query = Filestatus.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._1.value =!= "Success")
      .length
    db.run(query.result)
  }
}
