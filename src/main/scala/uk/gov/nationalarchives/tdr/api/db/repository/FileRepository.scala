package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Avmetadata, Consignment, Consignmentstatus, ConsignmentstatusRow, File, FileRow, Filemetadata, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.utils.DatabaseUtils.PgPositionedResult


import scala.concurrent.{ExecutionContext, Future}

class FileRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  implicit val getChildrenResult: GetResult[nationalarchives.Tables.FileRow] = GetResult(r => Tables.FileRow(
    r.nextUUID,
    r.nextUUID,
    r.nextUUID,
    r.nextTimestamp(),
    r.nextBooleanOption(),
    r.nextStringOption(),
    r.nextStringOption(),
    r.nextUUIDOption
  ))

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

  def getChildrenCTE(fileId: UUID): Future[Seq[Tables.FileRow]] = {
    val id = fileId.toString
    val sql =
      sql"""WITH RECURSIVE children AS (
           SELECT
            "FileId"::text,
            "ConsignmentId"::text,
            "UserId"::text,
            "Datetime",
            "ChecksumMatches",
            "FileType",
            "FileName",
            "ParentId"::text
           FROM "File"
            WHERE "FileId"::text = '#$id'
            UNION SELECT
             f."FileId"::text,
             f."ConsignmentId"::text,
             f."UserId"::text,
             f."Datetime",
             f."ChecksumMatches",
             f."FileType",
             f."FileName",
             f."ParentId"::text
            FROM "File" f INNER JOIN children c ON c."FileId"::text = f."ParentId"::text
        ) SELECT * FROM children;""".stripMargin.as[Tables.FileRow]
    db.run(sql)
  }

  def addFiles(fileRows: Seq[FileRow], consignmentStatusRow: ConsignmentstatusRow): Future[Seq[Tables.FileRow]] = {
    val allAdditions = DBIO.seq(insertFileQuery ++= fileRows, Consignmentstatus += consignmentStatusRow).transactionally
    db.run(allAdditions).map(_ => fileRows)
  }

  def addFiles(fileRows: Seq[FileRow], fileMetadataRows: Seq[FilemetadataRow]): Future[Unit] =
    db.run(DBIO.seq(File ++= fileRows, Filemetadata ++= fileMetadataRows).transactionally)

  def updateFileMetadata(fileMetadataRows: Seq[FilemetadataRow]): Future[Unit] =
    db.run(DBIO.seq(Filemetadata ++= fileMetadataRows))

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
}
