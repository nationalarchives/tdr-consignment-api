package uk.gov.nationalarchives.tdr.api.db

import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

import scala.util.{Failure, Success}

class DbConnection(dataSource: HikariCPJdbcDataSource) extends DbConnectionBase {
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

object DbConnection {
  def apply(dataSource: HikariCPJdbcDataSource): DbConnection = new DbConnection(dataSource)
}


//class DbConnection(slickSession: SlickSession) extends DbConnectionBase {
//  override def db: JdbcBackend#DatabaseDef = {
//    val db = slickSession.db
//    db.source match {
//      case hikariDataSource: HikariCPJdbcDataSource =>
//        val configBean = hikariDataSource.ds.getHikariConfigMXBean
//        getPassword match {
//          case Failure(exception) => throw exception
//          case Success(password) =>
//            configBean.setPassword(password)
//            db
//        }
//      case _ =>
//        db
//    }
//  }
//}
//object DbConnection {
//  def apply(slickSession: SlickSession): DbConnection = new DbConnection(slickSession)
//}
