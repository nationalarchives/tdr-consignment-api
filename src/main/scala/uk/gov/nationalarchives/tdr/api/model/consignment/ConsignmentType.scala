package uk.gov.nationalarchives.tdr.api.model.consignment

import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException

object ConsignmentType {
  val standard = "standard"
  val judgment = "judgment"

  implicit class ConsignmentTypeHelper(value: String) {
    def validateType: String = {
      value match {
        case _ if isStandard | isJudgment => value
        case _ => throw InputDataException(s"Invalid consignment type '$value' for consignment")
      }
    }

    def isJudgment: Boolean = {
      value == judgment
    }

    def isStandard: Boolean = {
      value == standard
    }
  }
}
