package uk.gov.nationalarchives.tdr.api.db.repository

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.utils.TestDatabase
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{createFileProperty, createFilePropertyDependencies, createFilePropertyValues}

class CustomMetadataPropertiesRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  "getClosureMetadataProperty" should "return the correct closure metadata property" in {
    val db = DbConnection.db
    val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
    deleteTables()
    createFileProperty("test", "desc", "test2", "Defined","text", true, false, "Mandatory Data" )
    val response = customMetadataPropertiesRepository.getClosureMetadataProperty.futureValue.head
    response.name should equal("test")
    response.description should equal(Some("desc"))
    response.fullname should equal(Some("test2"))
    response.propertytype should equal(Some("Defined"))
    response.datatype should equal(Some("text"))
    response.editable should equal(Some(true))
    response.mutlivalue should equal(Some(false))
    response.propertygroup should equal(Some("Mandatory Data"))
  }

  "getClosureMetadataValues" should "return the correct closure metadata values" in {
    val db = DbConnection.db
    val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
    createFilePropertyValues("LegalStatus","English", true, 0, 1)
    val response = customMetadataPropertiesRepository.getClosureMetadataValues.futureValue.head
    response.propertyname should equal("LegalStatus")
    response.propertyvalue should equal("English")
    response.default should equal(Some(true))
    response.dependencies should equal(Some(0))
    response.secondaryvalue should equal(Some(1))
  }

  "getClosureMetadataDependencies" should "return the correct closure metadata dependencies" in {
    val db = DbConnection.db
    val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
    createFilePropertyDependencies(1,"test","test2")
    val response = customMetadataPropertiesRepository.getClosureMetadataDependencies.futureValue.head
    response.groupid should equal(1)
    response.propertyname should equal("test")
    response.default should equal(Some("test2"))
  }
}
