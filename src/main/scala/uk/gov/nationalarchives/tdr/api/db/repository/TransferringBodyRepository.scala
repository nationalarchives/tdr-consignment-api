package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Body, BodyRow, Series}

import scala.concurrent.{ExecutionContext, Future}

class TransferringBodyRepository(db: Database) {

  def getTransferringBody(seriesId: UUID)(implicit executionContext: ExecutionContext): Future[BodyRow] = {
    val query = for {
      (body, _) <- Body.join(Series).on(_.bodyid === _.bodyid).filter(_._2.seriesid === seriesId)
    } yield body

    db.run(query.result).map(body => body.head)
  }
}
