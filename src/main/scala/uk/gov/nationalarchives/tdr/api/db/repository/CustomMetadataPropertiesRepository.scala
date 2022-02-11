package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Fileproperty, FilepropertyRow, Filepropertydependencies, FilepropertydependenciesRow,
  Filepropertyvalues, FilepropertyvaluesRow}

import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  def getClosureMetadataProperty: Future[Seq[FilepropertyRow]] = {
    val query = Fileproperty
    db.run(query.result)
  }

  def getClosureMetadataValues: Future[Seq[FilepropertyvaluesRow]] = {
    val query = Filepropertyvalues
    db.run(query.result)
  }

  def getClosureMetadataDependencies: Future[Seq[FilepropertydependenciesRow]] = {
    val query = Filepropertydependencies
    db.run(query.result)
  }
}
