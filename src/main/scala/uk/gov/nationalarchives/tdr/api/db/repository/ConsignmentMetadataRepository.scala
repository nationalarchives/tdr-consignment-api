package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Consignmentmetadata, _}

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {
  private val insertQuery = Consignmentmetadata returning Consignmentmetadata.map(_.metadataid) into
    ((consignmentMetadata, metadataid) => consignmentMetadata.copy(metadataid = metadataid))

  def addConsignmentMetadata(rows: Seq[ConsignmentmetadataRow]): Future[Seq[ConsignmentmetadataRow]] = {
    db.run(insertQuery ++= rows)
  }

  def getConsignmentMetadata(consignmentId: UUID, propertyNames: String*): Future[Seq[ConsignmentmetadataRow]] = {
    val query = Consignmentmetadata
      .filter(_.consignmentid === consignmentId)
      .filter(_.propertyname inSet propertyNames.toSet)
    db.run(query.result)
  }
}
