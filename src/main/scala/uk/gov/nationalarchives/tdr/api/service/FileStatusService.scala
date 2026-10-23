package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileStatusRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.{AddMultipleFileStatusesInput, FileStatus}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.common.utils.statuses.StatusTypes._
import uk.gov.nationalarchives.tdr.common.utils.statuses.StatusValues._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileStatusService(fileStatusRepository: FileStatusRepository)(implicit
    val executionContext: ExecutionContext
) {

  private def toFileStatuses(rows: Seq[FilestatusRow]): Seq[FileStatus] = {
    rows.map(r => FileStatus(r.fileid, r.statustype, r.value))
  }

  def addFileStatuses(addMultipleFileStatusesInput: AddMultipleFileStatusesInput): Future[List[FileStatus]] = {
    fileStatusRepository.addFileStatuses(addMultipleFileStatusesInput.statuses).map(_.map(row => FileStatus(row.fileid, row.statustype, row.value)).toList)
  }

  def getConsignmentFileProgress(consignmentId: UUID): Future[FileChecks] = {
    fileStatusRepository
      .getFileStatus(consignmentId, Set(FFIDType.id, ChecksumMatchType.id, AntivirusType.id))
      .map(rows => {
        val statusMap = rows.groupBy(_.statustype)
        FileChecks(
          AntivirusProgress(statusMap.getOrElse(AntivirusType.id, Nil).size),
          ChecksumProgress(statusMap.getOrElse(ChecksumMatchType.id, Nil).size),
          FFIDProgress(statusMap.getOrElse(FFIDType.id, Nil).size)
        )
      })
  }

  def getFileStatuses(consignmentId: UUID, statusTypes: Set[String], selectedFileIds: Option[Set[UUID]] = None): Future[List[FileStatus]] = {
    for {
      rows <- fileStatusRepository.getFileStatus(consignmentId, statusTypes, selectedFileIds)
    } yield toFileStatuses(rows).toList
  }

  def allChecksSucceeded(consignmentId: UUID): Future[Boolean] = {
    val statusTypes = Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id, RedactionType.id)
    fileStatusRepository
      .getFileStatus(consignmentId, statusTypes)
      .map(fileChecks => {
        !fileChecks.map(_.value).exists(_ != SuccessValue.value) &&
        Set(ChecksumMatchType.id, AntivirusType.id, FFIDType.id).forall(fileChecks.map(_.statustype).toSet.contains)
      })
  }
}

object FileStatusService {

  /** The status types returned by the `fileStatuses` GraphQL field.
    *
    * Note: this deliberately excludes `ClientFilePathType`, which is written and queried for file check failure reporting but has never been exposed as a file status. See
    * FileRepository.getFilesWithFileCheckFailures.
    */
  val allFileStatusTypes: Set[String] = fileStatusTypes.map(_.id)
}
