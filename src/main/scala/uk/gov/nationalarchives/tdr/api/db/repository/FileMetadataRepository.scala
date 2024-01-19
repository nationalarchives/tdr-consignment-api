package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.H2Profile.ProfileAction
import slick.jdbc.PostgresProfile.api._
import slick.model.Column
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{AddFileMetadataInput, ClientSideFileSize}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  private val insertFileMetadataQuery = Filemetadata.map(t => (t.fileid, t.value, t.userid, t.propertyname)) returning Filemetadata
    .map(r => (r.metadataid, r.datetime)) into ((fileMetadata, dbGeneratedValues) =>
    FilemetadataRow(dbGeneratedValues._1, fileMetadata._1, fileMetadata._2, dbGeneratedValues._2, fileMetadata._3, fileMetadata._4)
  )

  def getSumOfFileSizes(consignmentId: UUID): Future[Long] = {
    val query = Filemetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._1.propertyname === ClientSideFileSize)
      .map(_._1.value.asColumnOf[Long])
      .sum
      .getOrElse(0L)
    db.run(query.result)
  }

  def addFileMetadata(rows: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = {
    db.run(insertFileMetadataQuery ++= rows.map(i => (i.fileId, i.value, i.userId, i.filePropertyName)))
  }

  def getFileMetadataByProperty(fileIds: List[UUID], propertyName: String*): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .filter(_.fileid inSet fileIds)
      .filter(_.propertyname inSet propertyName.toSet)
    db.run(query.result)
  }

  def getFileMetadata(
                       consignmentId: Option[UUID],
                       selectedFileIds: Option[Set[UUID]] = None,
                       propertyNames: Option[Set[String]] = None,
                       searchTerm: Option[String] = None): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filterOpt(consignmentId)(_._2.consignmentid === _)
      .filterOpt(propertyNames)(_._1.propertyname inSetBind _)
      .filterOpt(selectedFileIds)(_._2.fileid inSetBind _)
      .filterOpt(searchTerm)(_._1.value like _)
      .map(_._1)
    db.run(query.result)
  }

  def updateFileMetadataProperties(selectedFileIds: Set[UUID], updatesByPropertyName: Map[String, FileMetadataUpdate]): Future[Seq[Int]] = {
    val dbUpdate: Seq[ProfileAction[Int, NoStream, Effect.Write]] = updatesByPropertyName.map { case (propertyName, update) =>
      Filemetadata
        .filter(fm => fm.fileid inSetBind selectedFileIds)
        .filter(fm => fm.propertyname === propertyName)
        .map(fm => (fm.value, fm.userid, fm.datetime))
        .update((update.value, update.userId, update.dateTime))
    }.toSeq

    db.run(DBIO.sequence(dbUpdate).transactionally)
  }

  def deleteFileMetadata(selectedFileIds: Set[UUID], propertyNames: Set[String]): Future[Int] = {
    val query = Filemetadata
      .filter(_.fileid inSetBind selectedFileIds)
      .filter(_.propertyname inSetBind propertyNames)
      .delete

    db.run(query)
  }

}

case class FileMetadataUpdate(metadataIds: Seq[UUID], filePropertyName: String, value: String, dateTime: Timestamp, userId: UUID)
