package uk.gov.nationalarchives.tdr.api.model.consignment

object ConsignmentType {
  val standard = "standard"
  val judgment = "judgment"

  implicit class consignmentTypeHelper(value: Option[String]) {
    def isJudgment: Boolean = {
      value.isDefined && value.get == judgment
    }

    def isStandard: Boolean = {
      value.isDefined && value.get == standard
    }
  }
}
