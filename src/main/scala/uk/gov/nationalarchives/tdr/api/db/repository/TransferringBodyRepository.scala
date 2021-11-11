package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Body, BodyRow, Series}

import scala.concurrent.{ExecutionContext, Future}

class TransferringBodyRepository(db: Database)(implicit executionContext: ExecutionContext) {

  def getTransferringBody(seriesId: UUID): Future[BodyRow] = {
    val query = for {
      (body, _) <- Body.join(Series).on(_.bodyid === _.bodyid).filter(_._2.seriesid === seriesId)
    } yield body

    db.run(query.result).map(body => body.head)
  }

  def getTransferringBodyByCode(code: String): Future[Option[BodyRow]] = {
    val query = Body.filter(_.tdrcode === code)
    db.run(query.result).map(body => body.headOption)
  }

  def dbHasTransferringBodies: Future[Boolean] = {
    val query = Body.length
    db.run(query.result).map(_ > 0)
  }
}
