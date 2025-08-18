package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Metadatareviewlog, MetadatareviewlogRow}

import scala.concurrent.{ExecutionContext, Future}

class MetadataReviewLogRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext)  {

  def addLogEntry(logRow: MetadatareviewlogRow): Future[MetadatareviewlogRow] = {
    val insert = Metadatareviewlog += logRow
    db.run(insert).map(_ => logRow)
  }
}
