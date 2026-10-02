package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.{JdbcBackend, RowsPerStatement}
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{File, Filestatus, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  // Each row binds 3 parameters and Postgres allows a maximum of 65535 bind parameters per statement
  private val maxRowsPerInsert = 5000

  private val insertQuery =
    Filestatus.map(t => (t.fileid, t.statustype, t.value)) returning Filestatus.map(r => (r.filestatusid, r.createddatetime)) into ((fileStatus, dbGeneratedValues) =>
      FilestatusRow(dbGeneratedValues._1, fileStatus._1, fileStatus._2, fileStatus._3, dbGeneratedValues._2)
    )

  def addFileStatuses(input: List[AddFileStatusInput]): Future[Seq[Tables.FilestatusRow]] = {
    val inserts = input
      .map(i => (i.fileId, i.statusType, i.statusValue))
      .grouped(maxRowsPerInsert)
      .map(batch => insertQuery.insertAll(batch, RowsPerStatement.All))
      .toList

    db.run(DBIO.sequence(inserts).map(_.flatten).transactionally)
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
