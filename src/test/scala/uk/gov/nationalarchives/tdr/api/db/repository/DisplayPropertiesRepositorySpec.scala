package uk.gov.nationalarchives.tdr.api.db.repository

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestContainerUtils, TestUtils}

import scala.concurrent.ExecutionContext

class DisplayPropertiesRepositorySpec extends TestContainerUtils with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  override def afterContainersStart(containers: containerDef.Container): Unit = super.afterContainersStart(containers)

  "getDisplayProperties" should "return all the display properties" in withContainers { case container: PostgreSQLContainer =>
    val db = container.database
    val utils = TestUtils(db)
    val displayPropertiesRepository = new DisplayPropertiesRepository(db)

    utils.createFileProperty("Language", "description", "Defined", "text", true, false, "group", "Language")
    utils.createDisplayProperty("Language", "Name", "Language", "text")
    utils.createDisplayProperty("Language", "Description", "description of language", "text")

    val result = displayPropertiesRepository.getDisplayProperties.futureValue
    result.size shouldBe 2
    val firstProperty = result.head
    firstProperty.propertyname.get should equal("Language")
    firstProperty.attribute.get should equal("Name")
    firstProperty.value.get should equal("Language")
    firstProperty.attributetype.get should equal("text")

    val lastProperty = result.tail.head
    lastProperty.propertyname.get should equal("Language")
    lastProperty.attribute.get should equal("Description")
    lastProperty.value.get should equal("description of language")
    lastProperty.attributetype.get should equal("text")
  }
}