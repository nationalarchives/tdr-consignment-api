package uk.gov.nationalarchives.tdr.api.model.consignment

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.ConsignmentTypeHelper

class ConsignmentTypeSpec extends AnyFlatSpec with ScalaFutures with Matchers {
  val standardType: String = "standard"
  val judgmentType: String = "judgment"
  val notRecognizedType: String = "notRecognizedType"

  "validateType" should "return 'true' if value is 'standard' or 'judgment'" in {
    val standardResult = standardType.validateType
    val judgmentResult = judgmentType.validateType

    standardResult shouldBe "standard"
    judgmentResult shouldBe "judgment"
  }

  "validateType" should "throw error is type is neither 'standard' or 'judgment'" in {
    val thrownException = intercept[Exception] {
      notRecognizedType.validateType
    }

    thrownException.getMessage should equal("Invalid consignment type 'notRecognizedType' for consignment")
  }

  "isStandard" should "should return 'true' if value is 'standard'" in {
    val result = standardType.isStandard

    result shouldBe true
  }

  "isStandard" should "should return 'false' if value is not 'standard'" in {
    val result = notRecognizedType.isStandard

    result shouldBe false
  }

  "isJudgment" should "should return 'true' if value is 'judgment'" in {
    val result = judgmentType.isJudgment

    result shouldBe true
  }

  "isJudgment" should "should return 'false' if value is not 'judgment'" in {
    val result = notRecognizedType.isJudgment

    result shouldBe false
  }

}
