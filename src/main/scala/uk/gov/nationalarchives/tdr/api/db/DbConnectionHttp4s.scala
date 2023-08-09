package uk.gov.nationalarchives.tdr.api.db

import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.util.{Failure, Success}

class DbConnectionHttp4s(dataSource: HikariCPJdbcDataSource) extends DbConnectionBase {
  override def db: JdbcBackend#DatabaseDef = {
    val configBean = dataSource.ds.getHikariConfigMXBean
    getPassword match {
      case Failure(exception) => throw exception
      case Success(password) =>
        configBean.setPassword(password)
        Database.forDataSource(dataSource.ds, Option(20))
    }
  }
}

object DbConnectionHttp4s {
  def apply(dataSource: HikariCPJdbcDataSource): DbConnectionHttp4s = new DbConnectionHttp4s(dataSource)
}
