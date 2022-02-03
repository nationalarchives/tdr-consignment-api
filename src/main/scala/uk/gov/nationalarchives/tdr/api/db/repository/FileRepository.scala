package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Avmetadata, Consignment, Consignmentstatus, ConsignmentstatusRow, File, FileRow, Filemetadata, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType

import scala.concurrent.{ExecutionContext, Future}

class FileRepository(db: Database)(implicit val executionContext: ExecutionContext) {
  private val insertFileQuery = File returning File.map(_.fileid)into ((file, fileid) => file.copy(fileid = fileid))

  def getFilesWithPassedAntivirus(consignmentId: UUID): Future[Seq[Tables.FileRow]] = {
    val query = Avmetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._2.filetype === NodeType.fileTypeIdentifier)
      .filter(_._1.result === "")
      .map(_._2)
    db.run(query.result)
  }

  def getConsignmentForFile(fileId: UUID): Future[Seq[Tables.ConsignmentRow]] = {
    val query = File.join(Consignment)
      .on(_.consignmentid === _.consignmentid)
      .filter(_._1.fileid === fileId)
      .map(rows => rows._2)
    db.run(query.result)
  }

  def addFiles(fileRows: Seq[FileRow], consignmentStatusRow: ConsignmentstatusRow): Future[Seq[Tables.FileRow]] = {
    val allAdditions = DBIO.seq(insertFileQuery ++= fileRows, Consignmentstatus += consignmentStatusRow).transactionally
    db.run(allAdditions).map(_ => fileRows)
  }

  def addFiles(fileRows: Seq[FileRow], fileMetadataRows: Seq[FilemetadataRow]): Future[Unit] =
    db.run(DBIO.seq(File ++= fileRows, Filemetadata ++= fileMetadataRows).transactionally)

  def countFilesInConsignment(consignmentId: UUID): Future[Int] = {
    val query = File.filter(_.consignmentid === consignmentId)
      .filter(_.filetype === NodeType.fileTypeIdentifier)
      .length
    db.run(query.result)
  }

  def countProcessedAvMetadataInConsignment(consignmentId: UUID): Future[Int] = {
    val query = Avmetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._2.filetype === NodeType.fileTypeIdentifier)
      .groupBy(_._1.fileid)
      .map(_._1)
      .length
    db.run(query.result)
  }

  def getFiles(consignmentId: UUID, parentId: Option[UUID], limit: Option[Int], after: Option[UUID]): Future[Seq[FileRow]] = {
    val query = File.filter(_.consignmentid === consignmentId)
      .filterOpt(parentId){ (f, id) => f.fileid === id || f.parentid === id }
      .filterOpt(after)(_.fileid > _)
      .sortBy(_.fileid)

    if (limit.isDefined) {
      query.take(limit.get)
    }

    db.run(query.result)
  }
}
