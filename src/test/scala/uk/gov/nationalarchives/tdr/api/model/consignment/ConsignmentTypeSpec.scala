package uk.gov.nationalarchives.tdr.api.model.consignment

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.consignmentTypeHelper

class ConsignmentTypeSpec extends AnyFlatSpec with ScalaFutures with Matchers {

  "isStandard" should "should return 'true' if value is 'standard'" in {
    val ct: Option[String] = Some("standard")

    val result = ct.isStandard

    result shouldBe true
  }

  "isStandard" should "should return 'false' if value is not 'standard'" in {
    val ct: Option[String] = Some("something")

    val result = ct.isStandard

    result shouldBe false
  }

  "isStandard" should "should return 'false' if value is not defined" in {
    val ct: Option[String] = None

    val result = ct.isStandard

    result shouldBe false
  }

  "isJudgment" should "should return 'true' if value is 'judgment'" in {
    val ct: Option[String] = Some("judgment")

    val result = ct.isJudgment

    result shouldBe true
  }

  "isJudgment" should "should return 'false' if value is not 'judgment'" in {
    val ct: Option[String] = Some("something")

    val result = ct.isJudgment

    result shouldBe false
  }

  "isJudgment" should "should return 'false' if value is not defined" in {
    val ct: Option[String] = None

    val result = ct.isJudgment

    result shouldBe false
  }

}
