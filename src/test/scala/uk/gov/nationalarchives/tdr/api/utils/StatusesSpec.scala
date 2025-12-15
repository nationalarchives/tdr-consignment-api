package uk.gov.nationalarchives.tdr.api.utils

import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.utils.Statuses._

class StatusesSpec extends AnyFlatSpec with MockitoSugar with Matchers {

  "'status types'" should "have the correct values assigned" in {
    AntivirusType.id should equal("Antivirus")
    AntivirusType.nonJudgmentStatus shouldBe false

    ChecksumMatchType.id should equal("ChecksumMatch")
    ChecksumMatchType.nonJudgmentStatus shouldBe false

    ClientChecksType.id should equal("ClientChecks")
    ClientChecksType.nonJudgmentStatus shouldBe false

    ClosureMetadataType.id should equal("ClosureMetadata")
    ClosureMetadataType.nonJudgmentStatus shouldBe true

    ConfirmTransferType.id should equal("ConfirmTransfer")
    ConfirmTransferType.nonJudgmentStatus shouldBe true

    DescriptiveMetadataType.id should equal("DescriptiveMetadata")
    DescriptiveMetadataType.nonJudgmentStatus shouldBe true

    DraftMetadataType.id should equal("DraftMetadata")
    DraftMetadataType.nonJudgmentStatus shouldBe true

    ExportType.id should equal("Export")
    ExportType.nonJudgmentStatus shouldBe false

    FFIDType.id should equal("FFID")
    FFIDType.nonJudgmentStatus shouldBe false

    MetadataReviewType.id should equal("MetadataReview")
    MetadataReviewType.nonJudgmentStatus shouldBe true

    RedactionType.id should equal("Redaction")
    RedactionType.nonJudgmentStatus shouldBe false

    SeriesType.id should equal("Series")
    SeriesType.nonJudgmentStatus shouldBe true

    ServerAntivirusType.id should equal("ServerAntivirus")
    ServerAntivirusType.nonJudgmentStatus shouldBe false

    ServerChecksumType.id should equal("ServerChecksum")
    ServerChecksumType.nonJudgmentStatus shouldBe false

    ServerFFIDType.id should equal("ServerFFID")
    ServerFFIDType.nonJudgmentStatus shouldBe false

    ServerRedactionType.id should equal("ServerRedaction")
    ServerRedactionType.nonJudgmentStatus shouldBe false

    TransferAgreementType.id should equal("TransferAgreement")
    TransferAgreementType.nonJudgmentStatus shouldBe true

    UploadType.id should equal("Upload")
    UploadType.nonJudgmentStatus shouldBe false
  }

  "'status values'" should "have the correct values assigned" in {
    CompletedValue.value should equal("Completed")
    CompletedWithIssuesValue.value should equal("CompletedWithIssues")
    FailedValue.value should equal("Failed")
    InProgressValue.value should equal("InProgress")
    MismatchValue.value should equal("Mismatch")
    NonJudgmentFormatValue.value should equal("NonJudgmentFormat")
    PasswordProtectedValue.value should equal("PasswordProtected")
    SuccessValue.value should equal("Success")
    VirusDetectedValue.value should equal("VirusDetected")
    ZeroByteFileValue.value should equal("ZeroByteFile")
    ZipValue.value should equal("Zip")
  }
}
