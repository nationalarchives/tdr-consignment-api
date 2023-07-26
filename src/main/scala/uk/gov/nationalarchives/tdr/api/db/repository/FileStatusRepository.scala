package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{File, Filestatus, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput

import java.util.UUID
import scala.concurrent.Future

class FileStatusRepository(db: Database) {
  private val insertQuery =
    Filestatus.map(t => (t.fileid, t.statustype, t.value)) returning Filestatus.map(r => (r.filestatusid, r.createddatetime)) into ((fileStatus, dbGeneratedValues) =>
      FilestatusRow(dbGeneratedValues._1, fileStatus._1, fileStatus._2, fileStatus._3, dbGeneratedValues._2)
      )

  def addFileStatuses(input: List[AddFileStatusInput]): Future[Seq[Tables.FilestatusRow]] = {
    db.run(insertQuery ++= input.map(i => (i.fileId, i.statusType, i.statusValue)))
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
