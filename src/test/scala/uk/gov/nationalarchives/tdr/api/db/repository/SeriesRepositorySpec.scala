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

  "getSeries" should "return the correct series for the given 'tdr body codes'" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val seriesRepository = new SeriesRepository(db)
    val seriesId1 = UUID.randomUUID()
    val seriesId2 = UUID.randomUUID()
    val bodyId1 = UUID.randomUUID()
    val bodyId2 = UUID.randomUUID()

    utils.addTransferringBody(bodyId1, "MOCK Department1", "Code123")
    utils.addTransferringBody(bodyId2, "MOCK Department2", "Code124")
    utils.addSeries(seriesId1, bodyId1, "TDR-2020-XYZ")
    utils.addSeries(seriesId2, bodyId2, "TDR-2020-ABC")

    val series: Seq[SeriesRow] = seriesRepository.getSeries(Seq("Code123", "Code124")).futureValue
    series.size shouldBe 2
    series.map(_.bodyid) shouldBe Seq(bodyId1, bodyId2)
    series.map(_.seriesid) shouldBe Seq(seriesId1, seriesId2)
    series.map(_.code) shouldBe Seq("TDR-2020-XYZ", "TDR-2020-ABC")
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
