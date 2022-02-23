package uk.gov.nationalarchives.tdr.api.utils

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.dimafeng.testcontainers.{ContainerDef, PostgreSQLContainer}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.utility.DockerImageName
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._

import java.sql.DriverManager
import java.util.UUID

trait TestContainerUtils extends AnyFlatSpec with TestContainerForEach {

  override val containerDef: ContainerDef = PostgreSQLContainer.Def(
    dockerImageName = DockerImageName.parse("ghcr.io/nationalarchives/tdr-consignment-api-data")
      .asCompatibleSubstituteFor("postgres"),
    databaseName = "consignmentapi",
    username = "tdr",
    password = "password"
  )

  def databaseUtils(container: PostgreSQLContainer): DatabaseUtils = {
    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
    val databaseUtils = DatabaseUtils(connection)
    databaseUtils
  }

  def setupBodyAndSeries(containers: containerDef.Container): Unit = {
    containers match {
      case psqlContainer: PostgreSQLContainer =>
        val utils = databaseUtils(psqlContainer)
        utils.addTransferringBody(
          fixedBodyId,
          "Default transferring body name",
          "default-transferring-body-code"
        )
        utils.addSeries(fixedSeriesId, fixedBodyId, "MOCK1")
    }
    super.afterContainersStart(containers)
  }

  def config(container: PostgreSQLContainer): Config = {
    ConfigFactory.load()
      .withValue("consignmentapi.db.url", ConfigValueFactory.fromAnyRef(container.jdbcUrl))
      .withValue("consignmentapi.db.driver", ConfigValueFactory.fromAnyRef("org.postgresql.Driver"))
      .withValue("consignmentapi.db.password", ConfigValueFactory.fromAnyRef("password"))
      .withValue("consignmentapi.db.user", ConfigValueFactory.fromAnyRef("tdr"))
      .withValue("consignmentapi.useIamAuth", ConfigValueFactory.fromAnyRef(false))
      .withValue("consignmentapi.profile", ConfigValueFactory.fromAnyRef("slick.jdbc.PostgresProfile$"))
      .withValue("consignmentapi.db.connectionPool", ConfigValueFactory.fromAnyRef("HikariCP"))
      .withValue("consignmentapi.db.keepAliveConnection", ConfigValueFactory.fromAnyRef(false))

  }

}
object TestContainerUtils {
  val fixedBodyId: UUID = UUID.fromString("4da472a5-16b3-4521-a630-5917a0722359")
  val fixedSeriesId: UUID = UUID.fromString("1436ad43-73a2-4489-a774-85fa95daff32")
}
