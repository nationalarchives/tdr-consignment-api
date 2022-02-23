package uk.gov.nationalarchives.tdr.api.db.repository.metadata

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.repository.CustomMetadataPropertiesRepository
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils

class CustomMetadataPropertiesRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override def afterContainersStart(containers: containerDef.Container): Unit = setupBodyAndSeries(containers)

  "getClosureMetadataProperty" should "return the correct closure metadata property" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db

      val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
      databaseUtils(container).createFileProperty("test", "desc", "test2", "Defined", "text", editable = true, mutlivalue = false, "Mandatory Data")
      val response = customMetadataPropertiesRepository.getClosureMetadataProperty.futureValue.head
      response.name should equal("test")
      response.description should equal(Some("desc"))
      response.propertytype should equal(Some("Defined"))
      response.datatype should equal(Some("text"))
      response.editable should equal(Some(true))
      response.mutlivalue should equal(Some(false))
      response.propertygroup should equal(Some("Mandatory Data"))
  }

  "getClosureMetadataValues" should "return the correct closure metadata values" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
      databaseUtils(container).createFilePropertyValues("LegalStatus", "English", default = true, 0, 1)
      val response = customMetadataPropertiesRepository.getClosureMetadataValues.futureValue.head
      response.propertyname should equal("LegalStatus")
      response.propertyvalue should equal("English")
      response.default should equal(Some(true))
      response.dependencies should equal(Some(0))
      response.secondaryvalue should equal(Some(1))
  }

  "getClosureMetadataDependencies" should "return the correct closure metadata dependencies" in withContainers {
    case container: PostgreSQLContainer =>
      val db = DbConnection(config(container)).db
      val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
      databaseUtils(container).createFilePropertyDependencies(1, "test", "test2")
      val response = customMetadataPropertiesRepository.getClosureMetadataDependencies.futureValue.head
      response.groupid should equal(1)
      response.propertyname should equal("test")
      response.default should equal(Some("test2"))
  }
}
