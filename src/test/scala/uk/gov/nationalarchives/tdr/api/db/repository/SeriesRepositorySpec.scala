package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.SeriesRow
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import java.util.UUID
import scala.concurrent.ExecutionContext

class SeriesRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "getSeries" should "return the correct series for the given 'tdr body code'" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val seriesRepository = new SeriesRepository(db)
    val seriesId = UUID.randomUUID()
    val bodyId = UUID.randomUUID()

    utils.addTransferringBody(bodyId, "MOCK Department", "Code123")
    utils.addSeries(seriesId, bodyId, "TDR-2020-XYZ")

    val series: Seq[SeriesRow] = seriesRepository.getSeries("Code123").futureValue
    series.size shouldBe 1
    series.head.bodyid shouldBe bodyId
    series.head.seriesid shouldBe seriesId
    series.head.code shouldBe "TDR-2020-XYZ"
  }

  "getSeries" should "return the correct series for the given 'series id'" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val seriesRepository = new SeriesRepository(db)
    val seriesId = UUID.randomUUID()
    val bodyId = UUID.randomUUID()

    utils.addTransferringBody(bodyId, "MOCK Department", "Code123")
    utils.addSeries(seriesId, bodyId, "TDR-2020-XYZ")

    val series: Seq[SeriesRow] = seriesRepository.getSeries(seriesId).futureValue
    series.size shouldBe 1
    series.head.bodyid shouldBe bodyId
    series.head.seriesid shouldBe seriesId
    series.head.code shouldBe "TDR-2020-XYZ"
  }
}
