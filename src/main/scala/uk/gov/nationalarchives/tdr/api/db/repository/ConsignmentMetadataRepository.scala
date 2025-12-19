package uk.gov.nationalarchives.tdr.api.db.repository

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxParallelTraverse1
import com.typesafe.config.ConfigFactory
import slick.jdbc.JdbcBackend

import java.util.UUID
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Consignmentmetadata, _}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.ConsignmentMetadataFilter

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentMetadataRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  private val insertQuery = Consignmentmetadata returning Consignmentmetadata.map(_.metadataid) into
    ((consignmentMetadata, metadataid) => consignmentMetadata.copy(metadataid = metadataid))

  private val batchSizeForConsignmentMetadataDatabaseWrites = ConfigFactory.load().getInt("consignmentMetadata.batchSize")
  private val maxConcurrencyForConsignmentMetadataDatabaseWrites = ConfigFactory.load().getInt("consignmentMetadata.maxConcurrency")

  def addConsignmentMetadata(rows: Seq[ConsignmentmetadataRow]): Future[Seq[ConsignmentmetadataRow]] = {
    val resultIO = Semaphore[IO](maxConcurrencyForConsignmentMetadataDatabaseWrites).flatMap { semaphore =>
      rows
        .grouped(batchSizeForConsignmentMetadataDatabaseWrites)
        .toList
        .parTraverse { br =>
          semaphore.permit.use { _ =>
            IO.fromFuture(IO(db.run(insertQuery ++= br)))
          }
        }
        .map(_.flatten)
    }
    resultIO.unsafeToFuture()
  }

  def deleteConsignmentMetadata(consignmentId: UUID, propertyNames: Set[String]): Future[Int] = {
    val query = Consignmentmetadata
      .filter(_.consignmentid === consignmentId)
      .filter(_.propertyname inSetBind propertyNames)
      .delete
    db.run(query)
  }

  def getConsignmentMetadata(consignmentId: UUID, filter: Option[ConsignmentMetadataFilter]): Future[Seq[ConsignmentmetadataRow]] = {
    val query = Consignmentmetadata
      .filter(_.consignmentid === consignmentId)
      .filterOpt(filter.map(_.propertyNames.toSet))(_.propertyname inSet _)
    db.run(query.result)
  }
}
