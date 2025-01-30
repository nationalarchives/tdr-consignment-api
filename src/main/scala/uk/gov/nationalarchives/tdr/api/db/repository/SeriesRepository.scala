package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables._
import slick.jdbc.JdbcBackend

import java.util.UUID
import scala.concurrent.Future

class SeriesRepository(db: JdbcBackend#Database) {

  def getSeries(tdrBodyCode: String): Future[Seq[SeriesRow]] = {
    val query = for {
      (series, _) <- Series.join(Body).on(_.bodyid === _.bodyid).filter(_._2.tdrcode === tdrBodyCode)
    } yield series
    db.run(query.result)
  }

  def getSeries(seriesId: UUID): Future[Seq[SeriesRow]] = {
    val query = Series.filter(_.seriesid === seriesId)

    db.run(query.result)
  }
}
