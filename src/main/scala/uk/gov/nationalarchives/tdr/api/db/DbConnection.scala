package uk.gov.nationalarchives.tdr.api.db

import slick.jdbc.JdbcBackend
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.util.{Failure, Success}

class DbConnection(databaseDef: JdbcBackend#DatabaseDef) extends DbConnectionBase {
  override def db: JdbcBackend#DatabaseDef = {
    databaseDef.source match {
      case hikariDataSource: HikariCPJdbcDataSource =>
        val configBean = hikariDataSource.ds.getHikariConfigMXBean
        getPassword match {
          case Failure(exception) => throw exception
          case Success(password) =>
            configBean.setPassword(password)
            databaseDef
        }
      case _ =>
        databaseDef
    }
  }
}

object DbConnection {
  def apply(databaseDef: JdbcBackend#DatabaseDef): DbConnection = new DbConnection(databaseDef)
}
