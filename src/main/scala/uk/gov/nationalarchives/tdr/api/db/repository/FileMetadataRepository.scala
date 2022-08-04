package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.SHA256ServerSideChecksum
import uk.gov.nationalarchives.tdr.api.model.file.NodeType

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  private val insertFileMetadataQuery = Filemetadata returning Filemetadata.map(_.metadataid) into
    ((filemetadata, metadataid) => filemetadata.copy(metadataid = metadataid))

  private val insertFileStatusQuery =
    Filestatus returning Filestatus.map(_.filestatusid) into ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def addFileMetadata(rows: Seq[FilemetadataRow]): Future[Seq[FilemetadataRow]] = {
    db.run(insertFileMetadataQuery ++= rows)
  }

  def addChecksumMetadata(fileMetadataRow: FilemetadataRow, fileStatusRow: FilestatusRow): Future[FilemetadataRow] = {
    val allUpdates = DBIO.seq(insertFileMetadataQuery += fileMetadataRow, insertFileStatusQuery += fileStatusRow).transactionally
    db.run(allUpdates).map(_ => fileMetadataRow)
  }

  def getFileMetadataByProperty(fileId: UUID, propertyName: String*): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .filter(_.fileid === fileId)
      .filter(_.propertyname inSet propertyName.toSet)
    db.run(query.result)
  }

  def getFileMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None, propertyNames: Option[Set[String]]=None): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filterOpt(propertyNames)(_._1.propertyname inSetBind _)
      .filterOpt(selectedFileIds)(_._2.fileid inSetBind _)
      .map(_._1)
    db.run(query.result)
  }

  def updateFileMetadata(update: FileMetadataUpdate): Future[Int] = {
    val filePropertyName = update.filePropertyName
    val metadataIds = update.metadataIds
    val value = update.value
    val userId = update.userId
    val dateTime = update.dateTime
    val dbUpdate = Filemetadata.filter(fm => fm.propertyname === filePropertyName)
      .filter(fm => fm.metadataid inSet metadataIds)
      .map(fm => (fm.value, fm.userid, fm.datetime))
      .update((value, userId, dateTime))
    db.run(dbUpdate)
  }

  def countProcessedChecksumInConsignment(consignmentId: UUID): Future[Int] = {
    val query = Filemetadata.join(File)
      .on(_.fileid === _.fileid).join(Fileproperty)
      .on(_._1.propertyname === _.name)
      .filter(_._1._2.consignmentid === consignmentId)
      .filter(_._2.name === SHA256ServerSideChecksum)
      .filter(_._1._2.filetype === NodeType.fileTypeIdentifier)
      .groupBy(_._1._2.fileid)
      .map(_._1)
      .length
    db.run(query.result)
  }
}

case class FileMetadataUpdate(metadataIds: Seq[UUID], filePropertyName: String, value: String, dateTime: Timestamp, userId: UUID)
