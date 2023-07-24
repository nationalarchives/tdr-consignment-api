package uk.gov.nationalarchives.tdr.api.model.consignment

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class ConsignmentReferenceSpec extends AnyFlatSpec with ScalaFutures with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  // scalastyle:off magic.number
  val testCases = List(
    ReferenceTestCase(2020, 5000L, "TDR-2020-LBB"),
    ReferenceTestCase(2020, 9223372036854775807L, "TDR-2020-JGXSCT7Q66HVKK"),
    ReferenceTestCase(2021, 123456L, "TDR-2021-K5RJ"),
    ReferenceTestCase(2022, 0, "TDR-2022-B")
  )

  testCases.foreach(testCase => {
    "createConsignmentReference" should s"generated consignment reference ${testCase.expectedReference} from " +
      s"year ${testCase.year} and sequence number ${testCase.sequenceNum}" in {

        val result: String = ConsignmentReference.createConsignmentReference(testCase.year, testCase.sequenceNum)

        result should equal(testCase.expectedReference)
      }
  })
}

case class ReferenceTestCase(year: Int, sequenceNum: Long, expectedReference: String)
