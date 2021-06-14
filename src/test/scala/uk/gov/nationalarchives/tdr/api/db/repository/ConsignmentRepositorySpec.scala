package uk.gov.nationalarchives.tdr.api.db.repository

import java.util.UUID

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.CurrentTimeSource
import uk.gov.nationalarchives.tdr.api.utils.{TestDatabase, TestUtils}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import scala.concurrent.ExecutionContext

class ConsignmentRepositorySpec extends AnyFlatSpec with TestDatabase with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  "addParentFolder" should "add parent folder name to an existing consignment row" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("0292019d-d112-465b-b31e-72dfb4d1254d")

    TestUtils.createConsignment(consignmentId, userId)

    consignmentRepository.addParentFolder(consignmentId, "TEST ADD PARENT FOLDER NAME").futureValue

    val parentFolderName = consignmentRepository.getConsignment(consignmentId).futureValue.map(consignment => consignment.parentfolder)

    parentFolderName should contain only Some("TEST ADD PARENT FOLDER NAME")
  }

  "getParentFolder" should "get parent folder name for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("b6da7577-3800-4ebc-821b-9d33e52def9e")

    TestUtils.createConsignment(consignmentId, userId)
    consignmentRepository.addParentFolder(consignmentId, "TEST GET PARENT FOLDER NAME").futureValue

    val parentFolderName = consignmentRepository.getParentFolder(consignmentId).futureValue

    parentFolderName should be(Some("TEST GET PARENT FOLDER NAME"))
  }

  "getParentFolder" should "return nothing if no parent folder exists" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("8233b9a4-5c2d-4c2d-9355-e6ec5751fea5")

    TestUtils.createConsignment(consignmentId, userId)

    val parentFolderName = consignmentRepository.getParentFolder(consignmentId).futureValue

    parentFolderName should be(None)
  }

  "getSeriesOfConsignment" should "get the series for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("b59a8bfd-5709-46c7-a5e9-71bae146e2f1")
    val seriesId = UUID.fromString("9e2e2a51-c2d0-4b99-8bef-2ca322528861")
    val bodyId = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
    val seriesCode = "Mock series"

    TestUtils.addSeries(seriesId, bodyId, seriesCode)
    TestUtils.createConsignment(consignmentId, userId)

    val consignmentSeries = consignmentRepository.getSeriesOfConsignment(consignmentId).futureValue.head

    consignmentSeries.code should be(seriesCode)
  }

  "getTransferringBodyOfConsignment" should "get the transferring body for a consignment" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentId = UUID.fromString("a3088f8a-59a3-4ab3-9e50-1677648e8186")
    val seriesId = UUID.fromString("845a4589-d412-49d7-80c6-63969112728a")
    val bodyId = UUID.fromString("edb31587-4357-4e63-b40c-75368c9d9cc9")
    val bodyName = "Some transferring body name"
    val seriesCode = "Mock series"

    TestUtils.addTransferringBody(bodyId, bodyName, "some-body-code")
    TestUtils.addSeries(seriesId, bodyId, seriesCode)
    TestUtils.createConsignment(consignmentId, userId, seriesId)

    val consignmentBody = consignmentRepository.getTransferringBodyOfConsignment(consignmentId).futureValue.head

    consignmentBody.name should be(bodyName)
  }

  "getNextConsignmentSequence" should "get the next sequence ID number for a consignment row" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentIdOne = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentIdTwo = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")

    TestUtils.createConsignment(consignmentIdOne, userId)
    TestUtils.createConsignment(consignmentIdTwo, userId)

    val sequenceId: Long = consignmentRepository.getNextConsignmentSequence.futureValue
    val expectedSeq = 3L

    sequenceId should be(expectedSeq)
  }

  "getConsignment" should "return the consignment given the consignment id" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentIdOne = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")

    TestUtils.createConsignment(consignmentIdOne, userId)

    val response = consignmentRepository.getConsignment(consignmentIdOne).futureValue

    response.size should be (1)
    response.headOption.get.consignmentid should equal(consignmentIdOne)
  }

  "getConsignments" should "return all consignments" in {
    val db = DbConnection.db
    val consignmentRepository = new ConsignmentRepository(db, new CurrentTimeSource)
    val consignmentIdOne = UUID.fromString("20fe77a7-51b3-434c-b5f6-a14e814a2e05")
    val consignmentIdTwo = UUID.fromString("fa19cd46-216f-497a-8c1d-6caaf3f421bc")

    TestUtils.createConsignment(consignmentIdOne, userId)
    TestUtils.createConsignment(consignmentIdTwo, userId)

    val response = consignmentRepository.getConsignments().futureValue

    response.size should be (2)
  }
}
