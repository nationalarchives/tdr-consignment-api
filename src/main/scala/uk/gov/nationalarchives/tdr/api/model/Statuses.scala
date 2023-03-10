package uk.gov.nationalarchives.tdr.api.model

object Statuses {
  trait StatusType {
    val id: String
  }

  trait ConsignmentType extends StatusType

  trait FileStatusType extends StatusType

  trait StatusValue {
    val value: String
  }

  def typesToId(statusTypes: List[StatusType]): List[String] = {
    statusTypes.map(_.id)
  }

  def toStatusType(statusType: String): StatusType = {
    statusType match {
      case AntivirusType.id           => AntivirusType
      case ChecksumMatchType.id       => ChecksumMatchType
      case ClosureMetadataType.id     => ClosureMetadataType
      case ClientChecksType.id        => ClientChecksType
      case ConfirmTransferType.id     => ConfirmTransferType
      case DescriptiveMetadataType.id => DescriptiveMetadataType
      case ExportType.id              => ExportType
      case FFIDType.id                => FFIDType
      case RedactionType.id           => RedactionType
      case SeriesType.id              => SeriesType
      case ServerAntivirusType.id     => ServerAntivirusType
      case ServerChecksumType.id      => ServerChecksumType
      case ServerFFIDType.id          => ServerFFIDType
      case TransferAgreementType.id   => TransferAgreementType
      case UploadType.id              => UploadType
      case _                          => Unrecognised
    }
  }

  def toStatusValue(statusValue: String): StatusValue = {
    statusValue match {
      case CompletedWithIssuesValue.value => CompletedWithIssuesValue
      case CompletedValue.value           => CompletedValue
      case EnteredValue.value             => EnteredValue
      case FailedValue.value              => FailedValue
      case IncompleteValue.value          => IncompleteValue
      case InProgressValue.value          => InProgressValue
      case MismatchValue.value            => MismatchValue
      case NotEnteredValue.value          => NotEnteredValue
      case NonJudgmentFormatValue.value   => NonJudgmentFormatValue
      case PasswordProtectedValue.value   => PasswordProtectedValue
      case SuccessValue.value             => SuccessValue
      case VirusDetectedValue.value       => VirusDetectedValue
      case ZeroByteFileValue.value        => ZeroByteFileValue
      case ZipValue.value                 => ZipValue
      case _                              => Unrecognised
    }
  }

  case object AntivirusType extends FileStatusType {
    val id: String = "Antivirus"
  }

  case object ChecksumMatchType extends FileStatusType {
    val id: String = "ChecksumMatch"
  }

  case object ClientChecksType extends ConsignmentType with FileStatusType {
    val id: String = "ClientChecks"
  }

  case object ClosureMetadataType extends ConsignmentType with FileStatusType {
    val id: String = "ClosureMetadata"
  }

  case object ConfirmTransferType extends ConsignmentType {
    val id: String = "ConfirmTransfer"
  }

  case object DescriptiveMetadataType extends ConsignmentType with FileStatusType {
    val id: String = "DescriptiveMetadata"
  }

  case object ExportType extends ConsignmentType {
    val id: String = "Export"
  }

  case object FFIDType extends FileStatusType {
    val id: String = "FFID"
  }

  case object RedactionType extends FileStatusType {
    val id: String = "Redaction"
  }

  case object SeriesType extends ConsignmentType {
    val id: String = "Series"
  }

  case object ServerAntivirusType extends ConsignmentType {
    val id: String = "ServerAntivirus"
  }

  case object ServerChecksumType extends ConsignmentType with FileStatusType {
    val id: String = "ServerChecksum"
  }

  case object ServerFFIDType extends ConsignmentType {
    val id: String = "ServerFFID"
  }

  case object TransferAgreementType extends ConsignmentType {
    val id: String = "TransferAgreement"
  }

  case object Unrecognised extends StatusType with StatusValue {
    val id, value: String = "Unrecognised"
  }

  case object UploadType extends ConsignmentType with FileStatusType {
    val id: String = "Upload"
  }

  case object CompletedValue extends StatusValue {
    val value: String = "Completed"
  }

  case object CompletedWithIssuesValue extends StatusValue {
    val value: String = "CompletedWithIssues"
  }

  case object EnteredValue extends StatusValue {
    val value: String = "Entered"
  }

  case object FailedValue extends StatusValue {
    val value: String = "Failed"
  }

  case object IncompleteValue extends StatusValue {
    val value: String = "Incomplete"
  }

  case object InProgressValue extends StatusValue {
    val value: String = "InProgress"
  }

  case object MismatchValue extends StatusValue {
    val value: String = "Mismatch"
  }

  case object NotEnteredValue extends StatusValue {
    val value: String = "NotEntered"
  }

  case object NonJudgmentFormatValue extends StatusValue {
    val value: String = "NonJudgmentFormat"
  }

  case object PasswordProtectedValue extends StatusValue {
    val value: String = "PasswordProtected"
  }

  case object SuccessValue extends StatusValue {
    val value: String = "Success"
  }

  case object VirusDetectedValue extends StatusValue {
    val value: String = "VirusDetected"
  }

  case object ZeroByteFileValue extends StatusValue {
    val value: String = "ZeroByteFile"
  }

  case object ZipValue extends StatusValue {
    val value: String = "Zip"
  }
}
