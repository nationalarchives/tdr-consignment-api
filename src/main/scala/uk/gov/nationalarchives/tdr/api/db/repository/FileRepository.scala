package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Avmetadata, Consignment, Consignmentstatus, ConsignmentstatusRow, File, FileRow, Filemetadata, FilemetadataRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.{FileRepositoryMetadata, RedactedFiles}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileRepository(db: Database)(implicit val executionContext: ExecutionContext) {
  implicit val getFileResult: GetResult[FileRow] = GetResult(r =>
    FileRow(
      UUID.fromString(r.nextString()),
      UUID.fromString(r.nextString()),
      UUID.fromString(r.nextString()),
      r.nextTimestamp(),
      r.nextBooleanOption(),
      r.nextStringOption(),
      r.nextStringOption(),
      r.nextStringOption().map(UUID.fromString)
    )
  )

  implicit val getRedactedFileResult: GetResult[RedactedFiles] = GetResult(r =>
    RedactedFiles(
      UUID.fromString(r.nextString()),
      r.nextString(),
      r.nextStringOption().map(UUID.fromString),
      r.nextStringOption()
    )
  )

  def getRedactedFilePairs(consignmentId: UUID, fileIds: Seq[UUID] = Nil, onlyNullValues: Boolean = false): Future[Seq[RedactedFiles]] = {
    val filterSuffix = if (fileIds.isEmpty) {
      ""
    } else {
      val fileString = fileIds.mkString("('", "','", "')")
      s"""AND "RedactedFileId" IN $fileString """
    }
    val whereSuffix = if (onlyNullValues) {
      """AND "FileId" IS NULL"""
    } else {
      ""
    }
    val idString = consignmentId.toString
    val regexp = "(_R\\d*$)"
    val similarTo = "%_R\\d*"
    val sql = sql"""SELECT "RedactedFileId", "RedactedFileName", "FileId", "FileName" FROM "File" RIGHT JOIN
        (
        select "ConsignmentId"::text AS "RedactedConsignmentId",
        "FileId"::text AS "RedactedFileId",
        "ParentId"::text "RedactedParentId",
        "FileName" AS "RedactedFileName",
        array_upper(string_to_array("FileName", '.'), 1) AS "ArrayLength",
        string_to_array("FileName", '.') AS "FileNameArray"
         from "File"
         WHERE "ConsignmentId"::text = $idString) AS RedactedFiles
            ON "ParentId"::text = "RedactedParentId"::text AND
             array_to_string(
                 array_append("FileNameArray"[0:"ArrayLength"-2], regexp_replace("FileNameArray"["ArrayLength"-1]::text, $regexp, '.')), '.'
                 ) = concat(array_to_string((string_to_array("FileName", '.'))[0:"ArrayLength"-1], '.'), '.')
             AND array_length(string_to_array("FileName", '.'), 1) > 1
            WHERE "FileNameArray"["ArrayLength" - 1] SIMILAR TO $similarTo  AND "ArrayLength" > 1 AND "FileNameArray"["ArrayLength"] != '' #$whereSuffix #$filterSuffix;
        """.stripMargin.as[RedactedFiles]
    db.run(sql)
  }

  private val insertFileQuery = File returning File.map(_.fileid) into ((file, fileid) => file.copy(fileid = fileid))

  def getFilesWithPassedAntivirus(consignmentId: UUID): Future[Seq[Tables.FileRow]] = {
    val query = Avmetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._2.filetype === NodeType.fileTypeIdentifier)
      .filter(_._1.result === "")
      .map(_._2)
    db.run(query.result)
  }

  def getConsignmentForFile(fileId: UUID): Future[Seq[Tables.ConsignmentRow]] = {
    val query = File
      .join(Consignment)
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

  def countFilesInConsignment(consignmentId: UUID, parentId: Option[UUID] = None, fileTypeIdentifier: Option[String] = Some(NodeType.fileTypeIdentifier)): Future[Int] = {
    val query = File
      .filter(_.consignmentid === consignmentId)
      .filterOpt(fileTypeIdentifier)(_.filetype === _)
      .filterOpt(parentId)(_.parentid === _)
      .length
    db.run(query.result)
  }

  def countProcessedAvMetadataInConsignment(consignmentId: UUID): Future[Int] = {
    val query = Avmetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._2.filetype === NodeType.fileTypeIdentifier)
      .groupBy(_._1.fileid)
      .map(_._1)
      .length
    db.run(query.result)
  }

  def getFiles(consignmentId: UUID, fileFilters: FileFilters): Future[Seq[FileRepositoryMetadata]] = {
    val query = File
      .joinLeft(Filemetadata)
      .on(_.fileid === _.fileid)
      .filter(_._1.consignmentid === consignmentId)
      .filterOpt(fileFilters.fileTypeIdentifier)(_._1.filetype === _)
      .filterOpt(fileFilters.selectedFileIds)(_._1.fileid inSet _)
      .map(res => (res._1, res._2))
    db.run(query.result)
  }

  def getPaginatedFiles(consignmentId: UUID, limit: Int, offset: Int, after: Option[String], fileFilters: FileFilters): Future[Seq[FileRow]] = {
    val query = File
      .filter(_.consignmentid === consignmentId)
      .filterOpt(fileFilters.parentId)(_.parentid === _)
      .filterOpt(after)(_.filename > _)
      .filterOpt(fileFilters.fileTypeIdentifier)(_.filetype === _)
      .sortBy(_.filename)
      .drop(offset)
      .take(limit)
    db.run(query.result)
  }

  def getAllDescendants(fileIds: Seq[UUID]): Future[Seq[FileRow]] = {

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
            WHERE "FileId"::text IN #${fileIds.mkString("('", "','", "')")}
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
        ) SELECT * FROM children;""".stripMargin.as[FileRow]
    db.run(sql)
  }

  def getConsignmentParentFolder(consignmentId: UUID): Future[Seq[Tables.FileRow]] = {
    val query = File
      .filter(_.consignmentid === consignmentId)
      .filter(_.parentid.isEmpty)
    db.run(query.result)
  }
}

case class FileMetadataFilters(closureMetadata: Boolean = false, descriptiveMetadata: Boolean = false)

case class FileFilters(
    fileTypeIdentifier: Option[String] = None,
    selectedFileIds: Option[List[UUID]] = None,
    parentId: Option[UUID] = None,
    metadataFilters: Option[FileMetadataFilters] = None
)

object FileRepository {
  case class RedactedFiles(redactedFileId: UUID, redactedFileName: String, fileId: Option[UUID], fileName: Option[String])
  type FileRepositoryMetadata = (FileRow, Option[FilemetadataRow])
}
