package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.MySQLProfile.api._
import uk.gov.nationalarchives.Tables._

import scala.concurrent.Future

class SeriesRepository(db: Database) {

  private val insertQuery = Series returning Series.map(_.seriesid) into ((series, seriesid) => series.copy(seriesid = Some(seriesid)))

  def getSeries(bodyName: String): Future[Seq[SeriesRow]] = {
    val query = for {
      (series, _) <- Series.join(Body).on(_.bodyid === _.bodyid).filter(_._2.name === bodyName)
    } yield series
    print(query.result.statements)
    db.run(query.result)

  }

  def addSeries(series: SeriesRow): Future[SeriesRow] = {
    db.run(insertQuery += series)
  }
}
