package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables._

import scala.concurrent.Future

class SeriesRepository(db: Database) {

  def getSeries(tdrBodyCode: String): Future[Seq[SeriesRow]] = {
    val query = for {
      (series, _) <- Series.join(Body).on(_.bodyid === _.bodyid).filter(_._2.tdrcode === tdrBodyCode)
    } yield series
    db.run(query.result)
  }
}
