package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Fileproperty, FilepropertyRow, Filepropertydependencies, FilepropertydependenciesRow, Filepropertyvalues, FilepropertyvaluesRow}

import scala.concurrent.{ExecutionContext, Future}

class CustomMetadataPropertiesRepository(db: JdbcBackend#Database)(implicit val executionContext: ExecutionContext) {

  @deprecated("Should use configuration schema and utils methods from da-metadata-schema: https://github.com/nationalarchives/da-metadata-schema/blob/main/config-schema/config.json")
  def getCustomMetadataProperty: Future[Seq[FilepropertyRow]] = {
    val query = Fileproperty
    db.run(query.result)
  }

  @deprecated("Should use configuration schema and utils methods from da-metadata-schema: https://github.com/nationalarchives/da-metadata-schema/blob/main/config-schema/config.json")
  def getCustomMetadataValues: Future[Seq[FilepropertyvaluesRow]] = {
    val query = Filepropertyvalues
    db.run(query.result)
  }

  @deprecated("Should use configuration schema and utils methods from da-metadata-schema: https://github.com/nationalarchives/da-metadata-schema/blob/main/config-schema/config.json")
  def getCustomMetadataValuesWithDefault: Future[Seq[FilepropertyvaluesRow]] = {
    val query = Filepropertyvalues.filter(_.default)
    db.run(query.result)
  }

  @deprecated("Should use configuration schema and utils methods from da-metadata-schema: https://github.com/nationalarchives/da-metadata-schema/blob/main/config-schema/config.json")
  def getCustomMetadataDependencies: Future[Seq[FilepropertydependenciesRow]] = {
    val query = Filepropertydependencies
    db.run(query.result)
  }
}
