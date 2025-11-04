package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.H2Profile.ProfileAction
import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.GetResult
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{AddFileMetadataInput, ClientSideFileSize, ClosureType}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {

  private val insertFileMetadataQuery = Filemetadata.map(t => (t.fileid, t.value, t.userid, t.propertyname)) returning Filemetadata
    .map(r => (r.metadataid, r.datetime)) into ((fileMetadata, dbGeneratedValues) =>
    FilemetadataRow(dbGeneratedValues._1, fileMetadata._1, fileMetadata._2, dbGeneratedValues._2, fileMetadata._3, fileMetadata._4)
  )

  // Helper to dedupe incoming rows (last value wins)
  private def dedupe(rows: Seq[AddFileMetadataInput]): Seq[AddFileMetadataInput] =
    rows.foldLeft(Map.empty[(UUID, String), AddFileMetadataInput]) { (acc, r) => acc + ((r.fileId -> r.filePropertyName) -> r) }.values.toSeq

  // Build a multi-row atomic upsert DBIO that deletes rows with empty values and upserts rows with non-empty values
  private def upsertFileMetadataAction(rows: Seq[AddFileMetadataInput]): DBIO[Seq[FilemetadataRow]] = {
    implicit val getResult: GetResult[(UUID, UUID, String, Timestamp, UUID, String)] =
      GetResult(r => (r.nextObject().asInstanceOf[UUID], r.nextObject().asInstanceOf[UUID], r.nextString(), r.nextTimestamp(), r.nextObject().asInstanceOf[UUID], r.nextString()))

    val deduped = dedupe(rows)
    if (deduped.isEmpty) DBIO.successful(Seq.empty)
    else {
      // Separate empty values (for deletion) from non-empty values (for upsert)
      val (emptyValueRows, nonEmptyValueRows) = deduped.partition(_.value.isEmpty)

      // Delete rows with empty values
      val deleteActions = emptyValueRows.map { row =>
        Filemetadata
          .filter(_.fileid === row.fileId)
          .filter(_.propertyname === row.filePropertyName)
          .delete
      }

      // Upsert rows with non-empty values
      val upsertAction = if (nonEmptyValueRows.nonEmpty) {
        val valuesClause = nonEmptyValueRows
          .map { r =>
            s"('${r.fileId}', '${r.value.replace("'", "''")}', '${r.userId}', '${r.filePropertyName.replace("'", "''")}')"
          }
          .mkString(", ")

        val sqlQuery = s"""
          INSERT INTO "FileMetadata" ("FileId", "Value", "UserId", "PropertyName")
          VALUES $valuesClause
          ON CONFLICT ("FileId", "PropertyName")
          DO UPDATE SET
            "Value" = EXCLUDED."Value",
            "UserId" = EXCLUDED."UserId",
            "Datetime" = CURRENT_TIMESTAMP
          RETURNING "MetadataId", "FileId", "Value", "Datetime", "UserId", "PropertyName"
        """

        sql"#$sqlQuery"
          .as[(UUID, UUID, String, Timestamp, UUID, String)]
          .map(_.map { case (metadataId, fileId, value, datetime, userId, propertyName) =>
            FilemetadataRow(metadataId, fileId, value, datetime, userId, propertyName)
          })
      } else {
        DBIO.successful(Seq.empty[FilemetadataRow])
      }

      // Execute deletes first, then upserts
      for {
        _ <- DBIO.sequence(deleteActions)
        upsertedRows <- upsertAction
      } yield upsertedRows
    }
  }

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
    addOrUpdateFileMetadata(rows)
  }

  def addOrUpdateFileMetadata(rows: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = {
    if (rows.isEmpty) Future.successful(Seq.empty)
    else db.run(upsertFileMetadataAction(rows).transactionally)
  }

  def getFileMetadataByProperty(fileIds: List[UUID], propertyName: String*): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .filter(_.fileid inSet fileIds)
      .filter(_.propertyname inSet propertyName.toSet)
    db.run(query.result)
  }

  def getFileMetadata(consignmentId: Option[UUID], selectedFileIds: Option[Set[UUID]] = None, propertyNames: Option[Set[String]] = None): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filterOpt(consignmentId)(_._2.consignmentid === _)
      .filterOpt(propertyNames)(_._1.propertyname inSetBind _)
      .filterOpt(selectedFileIds)(_._2.fileid inSetBind _)
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

  def totalClosedRecords(consignmentId: UUID): Future[Int] = {
    val query = Filemetadata
      .join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .filter(_._1.propertyname === ClosureType)
      .filter(_._1.value.toLowerCase === "closed")
      .length
    db.run(query.result)
  }

  def deleteFileMetadata(fileId: UUID, propertyNames: Set[String]): Future[Int] = {
    val query = Filemetadata
      .filter(_.fileid === fileId)
      .filter(_.propertyname inSetBind propertyNames)
      .delete

    db.run(query)
  }

}

case class FileMetadataUpdate(metadataIds: Seq[UUID], filePropertyName: String, value: String, dateTime: Timestamp, userId: UUID)
