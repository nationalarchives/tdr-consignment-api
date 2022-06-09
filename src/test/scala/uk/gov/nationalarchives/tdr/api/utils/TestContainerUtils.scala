package uk.gov.nationalarchives.tdr.api.utils

import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.dimafeng.testcontainers.{ContainerDef, PostgreSQLContainer}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.utility.DockerImageName
import slick.jdbc.JdbcBackend
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

trait TestContainerUtils extends AnyFlatSpec with TestContainerForEach with BeforeAndAfterEach {
  override val containerDef: ContainerDef = PostgreSQLContainer.Def(
    dockerImageName = DockerImageName.parse("tests")
      .asCompatibleSubstituteFor("postgres"),
    databaseName = "consignmentapi",
    username = "tdr",
    password = "password"
  )

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    containers match {
      case container: PostgreSQLContainer =>
        seedDatabase(container.database)
    }
    super.afterContainersStart(containers)
  }

  def seedDatabase(db: JdbcBackend#DatabaseDef): Unit = {
    setupBodyAndSeries(db)
  }

  def setupBodyAndSeries(db: JdbcBackend#DatabaseDef): Unit = {
    val utils = TestUtils(db)
    utils.addTransferringBody(
      fixedBodyId,
      "Default transferring body name",
      "default-transferring-body-code"
    )
    utils.addSeries(fixedSeriesId, fixedBodyId, "MOCK1")
  }
}

object TestContainerUtils {
  implicit class ContainerUtils(container: PostgreSQLContainer) {
    def setUrlProperty(): Unit = {
      System.setProperty("consignmentapi.db.url", container.jdbcUrl)
      ConfigFactory.invalidateCaches()
    }

    def session: SlickSession = {
      setUrlProperty()
      SlickSession.forConfig("consignmentapi")
    }

    def database: JdbcBackend#DatabaseDef = {
      session.db
    }
  }
}
