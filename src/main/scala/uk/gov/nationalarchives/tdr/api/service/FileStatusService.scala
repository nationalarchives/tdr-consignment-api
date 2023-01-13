package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{DisallowedPuidsRepository, FileRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, ChecksumMatch, FFID, Success}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileRepository: FileRepository, fileStatusRepository: FileStatusRepository, disallowedPuidsRepository: DisallowedPuidsRepository, uuidSource: UUIDSource)(
    implicit val executionContext: ExecutionContext
) {

  def addFileStatuses(addMultipleFileStatusesInput: AddMultipleFileStatusesInput): Future[List[FileStatus]] = {
    val rows = addMultipleFileStatusesInput.statuses.map(addFileStatusInput => {
      FilestatusRow(uuidSource.uuid, addFileStatusInput.fileId, addFileStatusInput.statusType, addFileStatusInput.statusValue, Timestamp.from(Instant.now()))
    })
    fileStatusRepository.addFileStatuses(rows).map(_.map(row => FileStatus(row.fileid, row.statustype, row.value)).toList)
  }

  @deprecated("Use addFileStatuses(addMultipleFileStatuses: List[AddFileStatusInput])")
  def addFileStatus(addFileStatusInput: AddFileStatusInput): Future[FileStatus] = {
    addFileStatuses(AddMultipleFileStatusesInput(addFileStatusInput :: Nil)).map(_.head)
  }

  def getFileStatus(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, String]] = {
    for {
      ffidStatus <- fileStatusRepository.getFileStatus(consignmentId, Set(FFID), selectedFileIds)
      fileStatusMap = ffidStatus.flatMap(fileStatusRow => Map(fileStatusRow.fileid -> fileStatusRow.value)).toMap
    } yield fileStatusMap
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    for {
      fileChecks <- fileStatusRepository.getFileStatus(consignmentId, Set(ChecksumMatch, Antivirus, FFID))
      fileChecksGroupedByStatusType: Map[String, Seq[FilestatusRow]] = fileChecks.groupBy(_.statustype)
      checksumMatchStatus: Seq[FilestatusRow] = fileChecksGroupedByStatusType.getOrElse(ChecksumMatch, Seq())
      avStatus: Seq[FilestatusRow] = fileChecksGroupedByStatusType.getOrElse(Antivirus, Seq())
      ffidStatusRows: Seq[FilestatusRow] = fileChecksGroupedByStatusType.getOrElse(FFID, Seq())
      ffidStatuses: Seq[String] = ffidStatusRows.map(_.value)
      failedFFIDStatuses <- disallowedPuidsRepository.activeReasons()
      failedRedactedFiles <- fileRepository.getRedactedFilePairs(consignmentId, onlyNullValues = true)
    } yield {
      failedRedactedFiles.isEmpty && checksumMatchStatus.nonEmpty && avStatus.nonEmpty && ffidStatuses.nonEmpty &&
      (checksumMatchStatus.filter(_.value != Success) ++ avStatus.filter(_.value != Success) ++ ffidStatuses.filter(failedFFIDStatuses.contains(_))).isEmpty
    }
  }
}

object FileStatusService {
  // Status types
  val ChecksumMatch = "ChecksumMatch"
  val Antivirus = "Antivirus"
  val FFID = "FFID"
  val Upload = "Upload"
  val ServerChecksum = "ServerChecksum"
  val ClientChecksum = "ClientChecksum"
  val ClientFilePath = "ClientFilePath"
  val ClientChecks = "ClientChecks"
  val ClosureMetadata = "ClosureMetadata"
  val DescriptiveMetadata = "DescriptiveMetadata"

  // Values
  val Success = "Success"
  val Failed = "Failed"
  val Mismatch = "Mismatch"
  val VirusDetected = "VirusDetected"
  val PasswordProtected = "PasswordProtected"
  val Zip = "Zip"
  val NonJudgmentFormat = "NonJudgmentFormat"
  val ZeroByteFile = "ZeroByteFile"
  val InProgress = "InProgress"
  val Completed = "Completed"
  val CompletedWithIssues = "CompletedWithIssues"
  val Incomplete = "Incomplete"
  val NotEntered = "NotEntered"

  val defaultStatuses: Map[String, String] = Map(ClosureMetadata -> NotEntered, DescriptiveMetadata -> NotEntered)
}
