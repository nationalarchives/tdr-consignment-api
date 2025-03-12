package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class Repository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {
  def loadCsvData(destinationTable: String, s3SourceUri: String): Future[Vector[String]] = {
    val plainSql =
      sql"""SELECT aws_s3.table_import_from_s3(
                          '$destinationTable',
                          '',
                          '(format csv)',
                          :'$s3SourceUri'
                        );"""
    val sql = plainSql.as[String]
    db.run(sql)
  }
}
