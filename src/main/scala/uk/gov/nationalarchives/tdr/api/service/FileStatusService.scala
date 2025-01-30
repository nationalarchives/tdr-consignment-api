package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.CustomMetadataField
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.AddOrUpdateFileMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddFileStatusInput, AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository, customMetadataService: CustomMetadataPropertiesService, displayPropertiesService: DisplayPropertiesService)(
    implicit val executionContext: ExecutionContext
) {

  private def toFileStatuses(rows: Seq[FilestatusRow]): Seq[FileStatus] = {
    rows.map(r => FileStatus(r.fileid, r.statustype, r.value))
  }

  def addFileStatuses(addMultipleFileStatusesInput: AddMultipleFileStatusesInput): Future[List[FileStatus]] = {
    fileStatusRepository.addFileStatuses(addMultipleFileStatusesInput.statuses).map(_.map(row => FileStatus(row.fileid, row.statustype, row.value)).toList)
  }

  def addAdditionalMetadataStatuses(fileMetadataList: Seq[AddOrUpdateFileMetadata]): Future[Seq[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      propertyNames <- displayPropertiesService.getActiveDisplayPropertyNames
      additionalMetadataStatuses = {
        val additionalMetadataGroups = customMetadataService.toAdditionalMetadataFieldGroups(customMetadataFields.filter(p => propertyNames.contains(p.name)))
        val metadataGroupsWithDefaultValues =
          additionalMetadataGroups.map(p => p.groupName -> p.fields.map(field => field.name -> field.defaultValue.getOrElse("")).toMap).toMap

        fileMetadataList.flatMap(fileMetadata => {
          metadataGroupsWithDefaultValues.map { case (groupName, fields) =>
            val hasDefaultValues = fields.forall(p => fileMetadata.metadata.find(_.filePropertyName == p._1).exists(_.value == p._2))
            val status = if (hasDefaultValues) {
              NotEntered
            } else {
              Completed
            }
            AddFileStatusInput(fileMetadata.fileId, groupName, status)
          }
        })
      }
      _ <- fileStatusRepository.deleteFileStatus(additionalMetadataStatuses.map(_.fileId).toSet, Set(ClosureMetadata, DescriptiveMetadata))
      rows <- fileStatusRepository.addFileStatuses(additionalMetadataStatuses.toList)
    } yield {
      rows
    }
  }

  def getConsignmentFileProgress(consignmentId: UUID): Future[FileChecks] = {
    fileStatusRepository
      .getFileStatus(consignmentId, Set(FFID, ChecksumMatch, Antivirus))
      .map(rows => {
        val statusMap = rows.groupBy(_.statustype)
        FileChecks(
          AntivirusProgress(statusMap.getOrElse(Antivirus, Nil).size),
          ChecksumProgress(statusMap.getOrElse(ChecksumMatch, Nil).size),
          FFIDProgress(statusMap.getOrElse(FFID, Nil).size)
        )
      })
  }

  def getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None): Future[List[FileStatus]] = {
    for {
      rows <- fileStatusRepository.getFileStatus(consignmentId, statusTypes, selectedFileIds)
    } yield toFileStatuses(rows).toList
  }

  @deprecated("Use getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None)")
  def getFileStatus(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, String]] = {
    for {
      ffidStatus <- getFileStatuses(consignmentId, Set(FFID), selectedFileIds)
      fileStatusMap = ffidStatus.flatMap(row => Map(row.fileId -> row.statusValue)).toMap
    } yield fileStatusMap
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    val statusTypes = Set(ChecksumMatch, Antivirus, FFID, Redaction)
    fileStatusRepository
      .getFileStatus(consignmentId, statusTypes)
      .map(fileChecks => {
        !fileChecks.map(_.value).exists(_ != Success) &&
        Set(ChecksumMatch, Antivirus, FFID).forall(fileChecks.map(_.statustype).toSet.contains)
      })
  }
}

object FileStatusService {
  // Status types
  val ChecksumMatch = "ChecksumMatch"
  val Antivirus = "Antivirus"
  val FFID = "FFID"
  val Redaction = "Redaction"
  val Upload = "Upload"
  val ServerChecksum = "ServerChecksum"
  val ClientChecks = "ClientChecks"
  val ClosureMetadata = "ClosureMetadata"
  val DescriptiveMetadata = "DescriptiveMetadata"

  val allFileStatusTypes: Set[String] = Set(ChecksumMatch, Antivirus, FFID, Redaction, Upload, ServerChecksum, ClientChecks, ClosureMetadata, DescriptiveMetadata)

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
  val Incomplete = "Incomplete"
  val NotEntered = "NotEntered"

  val defaultStatuses: Map[String, String] = Map(ClosureMetadata -> NotEntered, DescriptiveMetadata -> NotEntered)
}
