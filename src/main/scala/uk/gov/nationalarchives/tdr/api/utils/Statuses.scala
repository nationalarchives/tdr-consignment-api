package uk.gov.nationalarchives.tdr.api.utils

object Statuses {

  sealed trait StatusType {
    val id: String
    val nonJudgmentStatus: Boolean
  }

  sealed trait StatusValue { val value: String }

  case object SeriesType extends StatusType {
    val id: String = "Series"
    val nonJudgmentStatus: Boolean = true
  }

  case object UploadType extends StatusType {
    val id: String = "Upload"
    val nonJudgmentStatus: Boolean = false
  }

  case object TransferAgreementType extends StatusType {
    val id: String = "TransferAgreement"
    val nonJudgmentStatus: Boolean = true
  }

  case object ClientChecksType extends StatusType {
    val id: String = "ClientChecks"
    val nonJudgmentStatus: Boolean = false
  }

  case object ServerAntivirusType extends StatusType {
    val id: String = "ServerAntivirus"
    val nonJudgmentStatus: Boolean = false
  }

  case object ServerChecksumType extends StatusType {
    val id: String = "ServerChecksum"
    val nonJudgmentStatus: Boolean = false
  }

  case object ServerFFIDType extends StatusType {
    val id: String = "ServerFFID"
    val nonJudgmentStatus: Boolean = false
  }

  case object ServerRedactionType extends StatusType {
    val id: String = "ServerRedaction"
    val nonJudgmentStatus: Boolean = false
  }

  case object ConfirmTransferType extends StatusType {
    val id: String = "ConfirmTransfer"
    val nonJudgmentStatus: Boolean = true
  }

  case object ExportType extends StatusType {
    val id: String = "Export"
    val nonJudgmentStatus: Boolean = false
  }

  case object DraftMetadataType extends StatusType {
    val id: String = "DraftMetadata"
    val nonJudgmentStatus: Boolean = true
  }

  case object MetadataReviewType extends StatusType {
    val id: String = "MetadataReview"
    val nonJudgmentStatus: Boolean = true
  }

  case object ClosureMetadataType extends StatusType {
    val id: String = "ClosureMetadata"
    val nonJudgmentStatus: Boolean = true
  }

  case object DescriptiveMetadataType extends StatusType {
    val id: String = "DescriptiveMetadata"
    val nonJudgmentStatus: Boolean = true
  }

  case object ChecksumMatchType extends StatusType {
    val id: String = "ChecksumMatch"
    val nonJudgmentStatus: Boolean = false
  }

  case object AntivirusType extends StatusType {
    val id: String = "Antivirus"
    val nonJudgmentStatus: Boolean = false
  }

  case object FFIDType extends StatusType {
    val id: String = "FFID"
    val nonJudgmentStatus: Boolean = false
  }

  case object RedactionType extends StatusType {
    val id: String = "Redaction"
    val nonJudgmentStatus: Boolean = false
  }

  case object InProgressValue extends StatusValue { val value: String = "InProgress" }

  case object CompletedValue extends StatusValue { val value: String = "Completed" }

  case object CompletedWithIssuesValue extends StatusValue { val value: String = "CompletedWithIssues" }

  case object FailedValue extends StatusValue { val value: String = "Failed" }

  case object SuccessValue extends StatusValue { val value: String = "Success" }

  case object MismatchValue extends StatusValue { val value: String = "Mismatch" }

  case object VirusDetectedValue extends StatusValue { val value: String = "VirusDetected" }

  case object PasswordProtectedValue extends StatusValue { val value: String = "PasswordProtected" }

  case object ZipValue extends StatusValue { val value: String = "Zip" }

  case object NonJudgmentFormatValue extends StatusValue { val value: String = "NonJudgmentFormat" }

  case object ZeroByteFileValue extends StatusValue { val value: String = "ZeroByteFile" }
}
