package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import slick.jdbc.{GetResult, JdbcBackend}
import uk.gov
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Avmetadata, AvmetadataRow, Consignment, ConsignmentRow, Consignmentstatus, ConsignmentstatusRow, Ffidmetadata, FfidmetadataRow, Ffidmetadatamatches, FfidmetadatamatchesRow, File, FileRow, Filemetadata, FilemetadataRow, Filestatus, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileRepository.{FileFields, FileRepositoryMetadata}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.SHA256ClientSideChecksum
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, ChecksumMatch, ClientFilePath, FFID, Redaction, Success => FileCheckSuccess}

import java.time.ZonedDateTime
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.ZonedDateTimeUtils

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  implicit val getFileResult: GetResult[FileRow] = GetResult(r =>
    FileRow(
      UUID.fromString(r.nextString()),
      UUID.fromString(r.nextString()),
      UUID.fromString(r.nextString()),
      r.nextTimestamp(),
      r.nextBooleanOption(),
      r.nextStringOption(),
      r.nextStringOption(),
      r.nextStringOption().map(UUID.fromString),
      uploadmatchid = r.nextStringOption()
    )
  )

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
  
  def getFilesWithFileCheckFailures(
    consignmentId: Option[UUID],
    startDateTime: Option[ZonedDateTime],
    endDateTime: Option[ZonedDateTime]
  ): Future[Seq[((((((FileRow, ConsignmentRow), FilestatusRow), Option[AvmetadataRow]), Option[FfidmetadataRow]), Option[FfidmetadatamatchesRow]), Option[FilemetadataRow])]] = {
    val query =
      File
        .join(Consignment)
        .on(_.consignmentid === _.consignmentid)
        .join(Filestatus)
        .on(_._1.fileid === _.fileid)
        .joinLeft(Avmetadata)
        .on(_._1._1.fileid === _.fileid)
        .joinLeft(Ffidmetadata)
        .on(_._1._1._1.fileid === _.fileid)
        .joinLeft(Ffidmetadatamatches)
        .on(_._2.map(_.ffidmetadataid) === _.ffidmetadataid)
        .joinLeft(Filemetadata.filter(_.propertyname === SHA256ClientSideChecksum))
        .on(_._1._1._1._1._1.fileid === _.fileid)
        .filter(_._1._1._1._1._2.statustype inSetBind Set(Antivirus, FFID, ChecksumMatch, ClientFilePath, Redaction))
        .filter(_._1._1._1._1._2.value =!= FileCheckSuccess)
        .filterOpt(startDateTime.map(_.toTimestamp))(_._1._1._1._1._2.createddatetime > _)
        .filterOpt(endDateTime.map(_.toTimestamp))(_._1._1._1._1._2.createddatetime < _)
        .filterOpt(consignmentId)(_._1._1._1._1._1._1.consignmentid === _)
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

  def getFileFields(ids: Set[UUID]): Future[Seq[FileFields]] = {
    val query = File
      .filter(_.fileid inSet ids)
      .map(res => (res.fileid, res.filetype, res.userid, res.consignmentid, res.uploadmatchid))

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

  def getConsignmentParentFolder(consignmentId: UUID): Future[Seq[Tables.FileRow]] = {
    val query = File
      .filter(_.consignmentid === consignmentId)
      .filter(_.parentid.isEmpty)
    db.run(query.result)
  }
}

case class FileMetadataFilters(properties: Option[List[String]] = None)

case class FileFilters(
    fileTypeIdentifier: Option[String] = None,
    selectedFileIds: Option[List[UUID]] = None,
    parentId: Option[UUID] = None,
    metadataFilters: Option[FileMetadataFilters] = None
)

object FileRepository {
  type FileRepositoryMetadata = (FileRow, Option[FilemetadataRow])
  type FileFields = (UUID, Option[String], UUID, UUID, Option[String])
}
