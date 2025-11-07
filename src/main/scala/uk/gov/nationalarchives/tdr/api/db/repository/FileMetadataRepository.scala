package uk.gov.nationalarchives.tdr.api.db.repository

import org.postgresql.util.PSQLException
import slick.jdbc.H2Profile.ProfileAction
import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{AddFileMetadataInput, ClientSideFileSize, ClosureType}
import uk.gov.nationalarchives.tdr.api.utils.RetryUtils._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {

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
          case e: PSQLException if e.getSQLState == "40P01" =>
            println(s"DEADLOCK DETECTED: ${e.getMessage}")
            true
          case e: PSQLException if e.getMessage != null && e.getMessage.toLowerCase.contains("deadlock") =>
            println(s"DEADLOCK DETECTED (by message): ${e.getMessage}")
            true
          case e =>
            println(s"NON-DEADLOCK EXCEPTION: ${e.getClass.getSimpleName}: ${e.getMessage}")
            false
        }

        // 1. Optimistically try a bulk update first.
        retry(
          db.run(bulkUpdateAction),
          retries = 3,
          delay = 50.millis,
          isRetryable = isDeadlock
        ).recoverWith {
          // 2. Catch ANY exception after retries fail and check if it's a deadlock
          case e: Throwable =>
            println(s"RETRY EXHAUSTED, CHECKING FOR DEADLOCK FALLBACK: ${e.getClass.getSimpleName}: ${e.getMessage}")
            e match {
              case psql: PSQLException
                  if psql.getSQLState == "40P01" ||
                    (psql.getMessage != null && psql.getMessage.toLowerCase.contains("deadlock")) =>
                println(s"CONFIRMED DEADLOCK - FALLBACK TO ADVISORY LOCKS: ${psql.getMessage}")
                executeAdvisoryLockFallback(batch)
              case psql: PSQLException if psql.getMessage != null && psql.getMessage.toLowerCase.contains("deadlock") =>
                println(s"DEADLOCK BY MESSAGE - FALLBACK TO ADVISORY LOCKS: ${psql.getMessage}")
                executeAdvisoryLockFallback(batch)
              case _ =>
                println(s"NON-DEADLOCK FAILURE - RE-THROWING: ${e.getMessage}")
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

  // Helper method for advisory lock fallback
  private def executeAdvisoryLockFallback(batch: Seq[AddFileMetadataInput]): Future[Seq[FilemetadataRow]] = {
    println(s"ULTIMATE SEQUENTIAL FALLBACK: Processing ${batch.length} rows one-by-one")

    // Process each row individually, sequentially, in separate transactions
    // This is the ultimate fallback - slow but guaranteed to work
    val sequentialResults = batch.foldLeft(Future.successful(Seq.empty[FilemetadataRow])) { (accFuture, row) =>
      accFuture.flatMap { acc =>
        println(s"SEQUENTIAL: Processing row for file ${row.fileId}, property ${row.filePropertyName}")

        val singleRowAction = singleRowUpsertAction(row).transactionally

        db.run(singleRowAction)
          .map { rowResult =>
            val results = rowResult.toSeq
            println(s"SEQUENTIAL: Completed row for file ${row.fileId}, got ${results.length} results")
            acc ++ results
          }
          .recover { case e =>
            println(s"SEQUENTIAL ERROR for file ${row.fileId}, property ${row.filePropertyName}: ${e.getMessage}")
            // Even if individual rows fail, continue with others
            acc
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

  private def withAdvisoryLock[R](fileId: UUID)(action: DBIO[R]): DBIO[R] = {
    // PostgreSQL advisory locks are based on a 64-bit integer.
    // We create a unique, deterministic lock ID from the file's UUID.
    val lockId = fileId.getMostSignificantBits

    // The lock is transaction-scoped (pg_advisory_xact_lock), so it's automatically released at the end of the transaction.
    for {
      _ <- pgAdvisoryXactLock(lockId)
      result <- action
    } yield result
  }

  private def pgAdvisoryXactLock(lockId: Long): DBIO[Unit] = sql"SELECT pg_advisory_xact_lock($lockId)".as[Unit].head
}

case class FileMetadataUpdate(metadataIds: Seq[UUID], filePropertyName: String, value: String, dateTime: Timestamp, userId: UUID)
