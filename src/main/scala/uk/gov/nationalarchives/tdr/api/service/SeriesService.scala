package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.tdr.api.db.repository.SeriesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields.Series

import scala.concurrent.{ExecutionContext, Future}

class SeriesService(seriesRepository: SeriesRepository, uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {

  def getSeries(body: String): Future[Seq[Series]] = {
    val series = seriesRepository.getSeries(body)
    series.map(seriesRows => seriesRows.map(s => Series(s.seriesid, s.bodyid, s.name, s.code, s.description)))
  }
}
