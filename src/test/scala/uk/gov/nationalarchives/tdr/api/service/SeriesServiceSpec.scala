package uk.gov.nationalarchives.tdr.api.service

import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.SeriesRow
import uk.gov.nationalarchives.tdr.api.db.repository.SeriesRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import scala.concurrent.{ExecutionContext, Future}

class SeriesServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "getSeries" should "return the specfic series for a body if one is provided" in {
    val repoMock = setupSeriesResponses

    val seriesService: SeriesService = new SeriesService(repoMock)
    val seriesResponse: Seq[SeriesFields.Series] = seriesService.getSeries("1").await()

    verify(repoMock, times(1)).getSeries(anyString())
    seriesResponse.length should equal(1)
    checkFields(seriesResponse.head, SeriesCheck(1, 1, "name1", "code1", "description1"))
  }

  case class SeriesCheck(seriesId: Int, bodyId: Int, name: String, code: String, description: String)

  private def checkFields(series: SeriesFields.Series, seriesCheck: SeriesCheck) = {
    series.seriesid should equal(seriesCheck.seriesId)
    series.bodyid should equal(seriesCheck.bodyId)
    series.name.get should equal(seriesCheck.name)
    series.code.get should equal(seriesCheck.code)
    series.description.get should equal(seriesCheck.description)
  }

  private def setupSeriesResponses = {
    val seriesOne = SeriesRow(1, Option.apply("name1"), Option.apply("code1"), Option.apply("description1"), Some(1))
    val mockResponseOne: Future[Seq[SeriesRow]] = Future.successful(Seq(seriesOne))

    val repoMock = mock[SeriesRepository]
    when(repoMock.getSeries(anyString())).thenReturn(mockResponseOne)
    repoMock
  }
}
