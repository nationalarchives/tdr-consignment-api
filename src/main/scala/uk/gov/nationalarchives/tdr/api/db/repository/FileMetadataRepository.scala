package uk.gov.nationalarchives.tdr.api.db.repository

import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile.ProfileAction
import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{AddFileMetadataInput, ClientSideFileSize, ClosureType}
import uk.gov.nationalarchives.tdr.api.utils.RetryUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class FileMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DEADLOCK_SQL_STATE = "40P01" // PostgreSQL deadlock detected SQL state code https://www.postgresql.org/docs/current/errcodes-appendix.html

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

  def addFileMetadata(rows: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = addOrUpdateFileMetadata(rows)

  def addOrUpdateFileMetadata(rows: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = {
    if (rows.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      val deduped = dedupe(rows)
      val batchSize = 2000 // Process 5000 rows at a time
      val batches = deduped.grouped(batchSize).toSeq

      // Process each batch sequentially to avoid overwhelming the database
      val batchFutures = batches.map { batch =>
        val bulkUpdateAction = bulkUpsertAction(batch).transactionally

        val isDeadlock: Throwable => Boolean = {
          case e: PSQLException if e.getSQLState == DEADLOCK_SQL_STATE =>
            true
          case e: PSQLException if e.getMessage != null && e.getMessage.toLowerCase.contains("deadlock") =>
            true
          case _ =>
            false
        }
        // 1. Optimistically try a bulk update first.
        retry(
          db.run(bulkUpdateAction),
          retries = 3,
          delay = 50.millis,
          isRetryable = isDeadlock
        ).recoverWith { case e: Throwable =>
          e match {
            case psql: PSQLException
                if psql.getSQLState == DEADLOCK_SQL_STATE ||
                  (psql.getMessage != null && psql.getMessage.toLowerCase.contains("deadlock")) =>
              executeAdvisoryLockFallback(batch)
            case psql: PSQLException if psql.getMessage != null && psql.getMessage.toLowerCase.contains("deadlock") =>
              executeAdvisoryLockFallback(batch)
            case _ =>
              Future.failed(e)
          }
        }
      }
      Future.sequence(batchFutures).map(_.flatten)
    }
  }

  private def dedupe(rows: Seq[AddFileMetadataInput]): Seq[AddFileMetadataInput] =
    rows.foldLeft(Map.empty[(UUID, String), AddFileMetadataInput]) { (acc, r) => acc + ((r.fileId -> r.filePropertyName) -> r) }.values.toSeq

  implicit val getResult: slick.jdbc.GetResult[FilemetadataRow] =
    slick.jdbc.GetResult(r =>
      FilemetadataRow(
        r.nextObject().asInstanceOf[UUID],
        r.nextObject().asInstanceOf[UUID],
        r.nextString(),
        r.nextTimestamp(),
        r.nextObject().asInstanceOf[UUID],
        r.nextString()
      )
    )

  private def bulkUpsertAction(rows: Seq[AddFileMetadataInput]): DBIO[Seq[FilemetadataRow]] = {
    val (toDelete, toUpsert) = rows.partition(_.value.isEmpty)

    for {
      _ <-
        if (toDelete.nonEmpty) {
          // Create individual delete actions for each row
          val deleteActions = toDelete.map(row =>
            Filemetadata
              .filter(_.fileid === row.fileId)
              .filter(_.propertyname === row.filePropertyName)
              .delete
          )
          DBIO.sequence(deleteActions).map(_.sum)
        } else DBIO.successful(0)

      inserted <-
        if (toUpsert.nonEmpty) {
          val valuesList = toUpsert.map(row => s"('${row.fileId}', '${row.value.replace("'", "''")}', '${row.userId}', '${row.filePropertyName}')").mkString(", ")

          val upsertQuery =
            s"""INSERT INTO "FileMetadata" ("FileId", "Value", "UserId", "PropertyName")
             |VALUES $valuesList
             |ON CONFLICT ("FileId", "PropertyName") DO UPDATE SET
             |  "Value" = EXCLUDED."Value",
             |  "UserId" = EXCLUDED."UserId",
             |  "Datetime" = CURRENT_TIMESTAMP
             |RETURNING "MetadataId", "FileId", "Value", "Datetime", "UserId", "PropertyName";
             |""".stripMargin

          sql"""#$upsertQuery""".as[FilemetadataRow]
        } else DBIO.successful(Seq.empty)
    } yield inserted
  }

  private def executeAdvisoryLockFallback(batch: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = {
    val uniqueFileIds = batch.map(_.fileId).toSet
    logger.info(s"ADVISORY LOCK FALLBACK: Processing ${uniqueFileIds.size} files with proper advisory locks")

    // Process each file with its own advisory lock, sequentially to avoid lock conflicts
    val sequentialResults = uniqueFileIds.toList.foldLeft(Future.successful(Seq.empty[FilemetadataRow])) { (accFuture, fileId) =>
      accFuture.flatMap { acc =>
        val fileRows = batch.filter(_.fileId == fileId)
        val fileAction = withAdvisoryLock(fileId) {
          DBIO.sequence(fileRows.map(singleRowUpsertAction)).map(_.flatten)
        }.transactionally

        db.run(fileAction)
          .map { fileResults =>
            acc ++ fileResults
          }
          .recover { case e =>
            e match {
              case psql: PSQLException
                  if psql.getSQLState == DEADLOCK_SQL_STATE ||
                    (psql.getMessage != null && psql.getMessage.toLowerCase.contains("deadlock")) =>
                val rowResults = fileRows.foldLeft(acc) { (rowAcc, row) =>
                  val singleRowAction = singleRowUpsertAction(row).transactionally
                  try {
                    val future = db.run(singleRowAction)
                    val result = scala.concurrent.Await.result(future, 30.seconds)
                    rowAcc ++ result.toSeq
                  } catch {
                    case e: Exception =>
                      logger.warn(s"Failed to upsert row for FileId: ${row.fileId}, PropertyName: ${row.filePropertyName} due to: ${e.getMessage}")
                      rowAcc // Continue with other rows even if one fails
                  }
                }
                rowResults
              case _ =>
                throw e // Re-throw non-deadlock errors
            }
          }
      }
    }

    sequentialResults
  }

  // Single-row upsert action for the advisory lock fallback - much safer from deadlocks
  private def singleRowUpsertAction(row: AddFileMetadataInput): DBIO[Option[FilemetadataRow]] = {
    if (row.value.isEmpty) {
      // Delete single row using Slick DSL instead of raw SQL
      val deleteAction = Filemetadata
        .filter(_.fileid === row.fileId)
        .filter(_.propertyname === row.filePropertyName)
        .delete
      deleteAction.map(_ => None)
    } else {
      val fileIdStr = row.fileId.toString
      val valueStr = row.value.replace("'", "''")
      val userIdStr = row.userId.toString
      val propertyStr = row.filePropertyName

      val upsertQuery = s"""
        INSERT INTO "FileMetadata" ("FileId", "Value", "UserId", "PropertyName")
        VALUES ('$fileIdStr', '$valueStr', '$userIdStr', '$propertyStr')
        ON CONFLICT ("FileId", "PropertyName") DO UPDATE SET
          "Value" = EXCLUDED."Value",
          "UserId" = EXCLUDED."UserId",
          "Datetime" = CURRENT_TIMESTAMP
        RETURNING "MetadataId", "FileId", "Value", "Datetime", "UserId", "PropertyName"
      """

      sql"#$upsertQuery".as[FilemetadataRow].map(_.headOption)
    }
  }

  private def withAdvisoryLock[R](fileId: UUID)(action: DBIO[R]): DBIO[R] = {
    val lockId = Math.abs(fileId.hashCode().toLong)
    for {
      _ <- pgAdvisoryXactLock(lockId)
      result <- action
    } yield result
  }

  // PostgreSQL advisory lock helper methods
  private def pgAdvisoryXactLock(lockId: Long): DBIO[Unit] = sql"SELECT pg_advisory_xact_lock($lockId)".as[Unit].head

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
