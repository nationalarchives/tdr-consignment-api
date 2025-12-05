package uk.gov.nationalarchives.tdr.api.db.repository

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


  def addConsignmentMetadata(rows: Seq[ConsignmentmetadataRow]): Future[Seq[ConsignmentmetadataRow]] = {
    val batchedRows = rows.grouped(batchSizeForConsignmentMetadataDatabaseWrites).toIndexedSeq
    Future.traverse(batchedRows) { br =>
      db.run(insertQuery ++= br)
    }.map(_.flatten)
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
