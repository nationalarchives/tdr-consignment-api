package uk.gov.nationalarchives.tdr.api.db

import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.util.{Failure, Success}

class DbConnectionHttp4s() extends DbConnectionBase {
  override def db: JdbcBackend#DatabaseDef = {
    val db = Database.forConfig("consignmentapi.db")
    db.source match {
      case hikariDataSource: HikariCPJdbcDataSource =>
        val configBean = hikariDataSource.ds.getHikariConfigMXBean
        getPassword match {
          case Failure(exception) => throw exception
          case Success(password) =>
            configBean.setPassword(password)
            db
        }
      case _ =>
        db
    }
  }
}

object DbConnectionHttp4s {
  def apply(): DbConnectionHttp4s = new DbConnectionHttp4s()
}