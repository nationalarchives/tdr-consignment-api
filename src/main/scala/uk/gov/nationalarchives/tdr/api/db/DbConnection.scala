package uk.gov.nationalarchives.tdr.api.db

import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile

object DbConnection {
  private val defaultNumberOfThreads = 20

  def slickSession(config: Config = ConfigFactory.load): SlickSession = {
    if (config.getBoolean("consignmentapi.useIamAuth")) {
      val dbConfig = config.getConfig("consignmentapi.db")
      val numThreads = if (dbConfig.hasPath("numThreads")) dbConfig.getInt("numThreads") else defaultNumberOfThreads
      val dataSource = new IamAuthDataSource(dbConfig.getString("url"), dbConfig.getString("user"))

      val hikariConfig = new HikariConfig()
      hikariConfig.setPoolName("consignmentapi.db")
      hikariConfig.setDataSource(dataSource)
      hikariConfig.setMaximumPoolSize(numThreads)
      hikariConfig.setMinimumIdle(numThreads)

      SlickSession.forDbAndProfile(Database.forDataSource(new HikariDataSource(hikariConfig), Some(numThreads)), PostgresProfile)
    } else {
      SlickSession.forConfig("consignmentapi")
    }
  }
}
