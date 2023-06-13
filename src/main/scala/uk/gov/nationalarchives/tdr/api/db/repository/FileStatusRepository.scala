package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.H2Profile.ProfileAction
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.Future

class FileStatusRepository(db: Database) {
  private val insertQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((fileStatus, filestatusid) => fileStatus.copy(filestatusid = filestatusid))

  def upsertFileStatuses(fileStatusRows: List[FilestatusRow]): Future[Seq[Int]] = {
    val dbUpsert: Seq[ProfileAction[Int, NoStream, Effect.Write]] = fileStatusRows.map { row =>
      Filestatus.insertOrUpdate(row)
    }

    db.run(DBIO.sequence(dbUpsert).transactionally)
  }
  def addFileStatuses(fileStatusRows: List[FilestatusRow]): Future[Seq[Tables.FilestatusRow]] = {
    db.run(insertQuery ++= fileStatusRows)
  }

  def getFileStatus(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None): Future[Seq[FilestatusRow]] = {
    val query = Filestatus
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._1.statustype inSetBind statusTypes)
      .filterOpt(selectedFileIds)(_._2.fileid inSetBind _)
      .map(_._1)
    db.run(query.result)
  }

  def deleteFileStatus(selectedFileIds: Set[UUID], statusType: Set[String]): Future[Int] = {
    val query = Filestatus
      .filter(_.fileid inSetBind selectedFileIds)
      .filter(_.statustype inSetBind statusType)
      .delete

    db.run(query)
  }
}
