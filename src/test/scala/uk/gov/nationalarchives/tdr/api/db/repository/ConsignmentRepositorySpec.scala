package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.CurrentTimeSource
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestUtils}

import scala.concurrent.ExecutionContext

class ConsignmentRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val consignmentIdOne: UUID = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
  val consignmentIdTwo: UUID = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")
  val consignmentIdThree: UUID = UUID.fromString("614d0cba-380f-4b09-a6e4-542413dd7f4a")
  val consignmentIdFour: UUID = UUID.fromString("47019574-8407-40c7-b618-bf2b8f8b0de7")

  "addParentFolder" should "add parent folder name to an existing consignment row" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    TestUtils.createConsignment(consignmentIdOne, userId)

    consignmentRepository.addParentFolder(consignmentIdOne, "TEST ADD PARENT FOLDER NAME").futureValue

    val parentFolderName = consignmentRepository.getConsignment(consignmentIdOne).futureValue.map(consignment => consignment.parentfolder)

    parentFolderName should contain only Some("TEST ADD PARENT FOLDER NAME")
  }

  "getParentFolder" should "get parent folder name for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    TestUtils.createConsignment(consignmentIdOne, userId)
    consignmentRepository.addParentFolder(consignmentIdOne, "TEST GET PARENT FOLDER NAME").futureValue

    val parentFolderName = consignmentRepository.getParentFolder(consignmentIdOne).futureValue

    parentFolderName should be(Some("TEST GET PARENT FOLDER NAME"))
  }

  "getParentFolder" should "return nothing if no parent folder exists" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    TestUtils.createConsignment(consignmentIdOne, userId)

    val parentFolderName = consignmentRepository.getParentFolder(consignmentIdOne).futureValue

    parentFolderName should be(None)
  }

  "getSeriesOfConsignment" should "get the series for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val seriesId = UUID.fromString("9e2e2a51-c2d0-4b99-8bef-2ca322528861")
    val bodyId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val seriesCode = "Mock series"

    TestUtils.addSeries(seriesId, bodyId, seriesCode)
    TestUtils.createConsignment(consignmentIdOne, userId)

    val consignmentSeries = consignmentRepository.getSeriesOfConsignment(consignmentIdOne).futureValue.head

    consignmentSeries.code should be(seriesCode)
  }

  "getTransferringBodyOfConsignment" should "get the transferring body for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val seriesId = UUID.fromString("845a4589-d412-49d7-80c6-63969112728a")
    val bodyId = UUID.fromString("edb31587-4357-4e63-b40c-75368c9d9cc9")
    val bodyName = "Some transferring body name"
    val seriesCode = "Mock series"

    TestUtils.addTransferringBody(bodyId, bodyName, "some-body-code")
    TestUtils.addSeries(seriesId, bodyId, seriesCode)
    TestUtils.createConsignment(consignmentIdOne, userId, seriesId)

    val consignmentBody = consignmentRepository.getTransferringBodyOfConsignment(consignmentIdOne).futureValue.head

    consignmentBody.name should be(bodyName)
  }

  "getNextConsignmentSequence" should "get the next sequence ID number for a consignment row" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    TestUtils.createConsignment(consignmentIdOne, userId)
    TestUtils.createConsignment(consignmentIdTwo, userId)

    val sequenceId: Long = consignmentRepository.getNextConsignmentSequence.futureValue
    val expectedSeq = 3L

    sequenceId should be(expectedSeq)
  }

  "getConsignment" should "return the consignment given the consignment id" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    TestUtils.createConsignment(consignmentIdOne, userId)

    val response = consignmentRepository.getConsignment(consignmentIdOne).futureValue

    response should have size 1
    response.headOption.get.consignmentid should equal(consignmentIdOne)
  }

  "getConsignments" should "return all consignments including cursor value up to the limit value" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    createConsignments()

    val response = consignmentRepository.getConsignments(2, "TDR-2021-B").futureValue

    response should have size 2
    val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
    consignmentIds should contain (consignmentIdTwo)
    consignmentIds should contain (consignmentIdThree)
  }

  "getConsignments" should "return all consignments up to limit where empty cursor provided" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    createConsignments()

    val response = consignmentRepository.getConsignments(2, "").futureValue
    response should have size 2
    val consignmentIds: List[UUID] = response.map(cr => cr.consignmentid).toList
    consignmentIds should contain (consignmentIdOne)
    consignmentIds should contain (consignmentIdTwo)
  }

  "getConsignments" should "return no consignments where limit set at '0'" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    createConsignments()

    val response = consignmentRepository.getConsignments(0, "TDR-2021-A").futureValue
    response should have size 0
  }

  "getConsignments" should "return no consignments where non-existent cursor value provided'" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    createConsignments()

    val response = consignmentRepository.getConsignments(0, "Some-Non-existent-cursor").futureValue
    response should have size 0
  }

  "getConsignments" should "return no consignments where there are no consignments" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)

    val response = consignmentRepository.getConsignments(2, "").futureValue
    response should have size  0
  }

  private def createConsignments(): Unit = {
    TestUtils.createConsignment(consignmentIdOne, userId, consignmentRef = "TDR-2021-A")
    TestUtils.createConsignment(consignmentIdTwo, userId, consignmentRef = "TDR-2021-B")
    TestUtils.createConsignment(consignmentIdThree, userId, consignmentRef = "TDR-2021-C")
    TestUtils.createConsignment(consignmentIdFour, userId, consignmentRef = "TDR-2021-D")
  }
}
