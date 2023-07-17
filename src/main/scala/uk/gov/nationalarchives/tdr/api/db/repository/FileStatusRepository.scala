package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{File, Filestatus, FilestatusRow}

import java.util.UUID
import scala.concurrent.Future

class FileStatusRepository(db: Database) {
  private val insertQuery = Filestatus returning Filestatus.map(_.filestatusid) into
    ((fileStatus, filestatusid) => fileStatus.copy(filestatusid = filestatusid))

  private val insertQueryV2 =
    Filestatus.map(t => (t.fileid, t.statustype, t.value, t.createddatetime)) returning Filestatus.map(_.filestatusid) into ((fileStatus, generatedFileStatusId) =>
      FilestatusRow(generatedFileStatusId, fileStatus._1, fileStatus._2, fileStatus._3, fileStatus._4)
    )

  def addFileStatuses(fileStatusRows: List[FilestatusRow]): Future[Seq[Tables.FilestatusRow]] = {
    db.run(insertQueryV2 ++= fileStatusRows.map(r => (r.fileid, r.statustype, r.value, r.createddatetime)))
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

//class X(tag: Tag) extends Table[Filestatus](tag, "Filestatus") {
//  def fileId = column[UUID]("fileid", O.PrimaryKey, O.AutoInc)
//
//  def name = column[String]("name")
//  override def * : ProvenShape[nationalarchives.Tables.Filestatus] = fileId ~ name
//}

//object X extends Table[(UUID, UUID, String, String, java.sql.Timestamp)]("filestatus") {
//  def id = column[UUID]("id", O.PrimaryKey, O.AutoInc)
//  def name = column[String]("name")
//
//  def * = id ~ name // Note: Just a simple projection, not using .? etc
//}
