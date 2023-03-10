package uk.gov.nationalarchives.tdr.api.model

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatusesSpec extends AnyFlatSpec with ScalaFutures with Matchers {

  "typesToId" should "return the ids of the provided status types" in {
    val result = Statuses.typesToId(List(Statuses.AntivirusType, Statuses.ConfirmTransferType))
    result.size shouldBe 2
    result.head should equal("Antivirus")
    result.tail.head should equal("ConfirmTransfer")
  }

  "StatusTypes" should "return the correct 'id'" in {
    Statuses.AntivirusType.id should equal("Antivirus")
    Statuses.ChecksumMatchType.id should equal("ChecksumMatch")
    Statuses.ClientChecksType.id should equal("ClientChecks")
    Statuses.ClosureMetadataType.id should equal("ClosureMetadata")
    Statuses.ConfirmTransferType.id should equal("ConfirmTransfer")
    Statuses.DescriptiveMetadataType.id should equal("DescriptiveMetadata")
    Statuses.ExportType.id should equal("Export")
    Statuses.FFIDType.id should equal("FFID")
    Statuses.RedactionType.id should equal("Redaction")
    Statuses.SeriesType.id should equal("Series")
    Statuses.ServerAntivirusType.id should equal("ServerAntivirus")
    Statuses.ServerChecksumType.id should equal("ServerChecksum")
    Statuses.ServerFFIDType.id should equal("ServerFFID")
    Statuses.TransferAgreementType.id should equal("TransferAgreement")
    Statuses.UploadType.id should equal("Upload")
  }

  "StatusValues" should "return the correct 'value'" in {
    Statuses.CompletedValue.value should equal("Completed")
    Statuses.CompletedWithIssuesValue.value should equal("CompletedWithIssues")
    Statuses.EnteredValue.value should equal("Entered")
    Statuses.FailedValue.value should equal("Failed")
    Statuses.IncompleteValue.value should equal("Incomplete")
    Statuses.InProgressValue.value should equal("InProgress")
    Statuses.MismatchValue.value should equal("Mismatch")
    Statuses.NotEnteredValue.value should equal("NotEntered")
    Statuses.NonJudgmentFormatValue.value should equal("NonJudgmentFormat")
    Statuses.PasswordProtectedValue.value should equal("PasswordProtected")
    Statuses.SuccessValue.value should equal("Success")
    Statuses.VirusDetectedValue.value should equal("VirusDetected")
    Statuses.ZeroByteFileValue.value should equal("ZeroByteFile")
    Statuses.ZipValue.value should equal("Zip")
  }
}
