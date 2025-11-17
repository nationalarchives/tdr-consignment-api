package uk.gov.nationalarchives.tdr.api.graphql

import sangria.execution.deferred.{Deferred, UnsupportedDeferError}
import uk.gov.nationalarchives.tdr.api.db.repository.FileFilters
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentMetadata, ConsignmentMetadataFilter, FileChecks, PaginationInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.ConsignmentStatus
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.File
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class DeferredResolver extends sangria.execution.deferred.DeferredResolver[ConsignmentApiContext] {
  // We may at some point need to do authorisation in this method. There is a ensurePermissions method which needs to be called before returning data.
  // scalastyle:off cyclomatic.complexity
  override def resolve(deferred: Vector[Deferred[Any]], context: ConsignmentApiContext, queryState: Any)(implicit ec: ExecutionContext): Vector[Future[Any]] = {
    // Extract all consignment IDs for status requests to batch them
    val consignmentStatusDefers = deferred.collect { case d: DeferConsignmentStatuses => d }

    // If there are multiple consignment status requests, batch them
    val consignmentStatusFuture: Future[Map[UUID, List[ConsignmentStatus]]] = if (consignmentStatusDefers.nonEmpty) {
      val consignmentIds = consignmentStatusDefers.map(_.consignmentId).distinct
      context.consignmentStatusService.getConsignmentStatusesByConsignmentIds(consignmentIds).map { rows =>
        rows.groupBy(_.consignmentid).map { case (id, statusRows) =>
          id -> statusRows
            .map(row =>
              ConsignmentStatus(
                row.consignmentstatusid,
                row.consignmentid,
                row.statustype,
                row.value,
                row.createddatetime.toZonedDateTime,
                row.modifieddatetime.map(timestamp => timestamp.toZonedDateTime)
              )
            )
            .toList
        }
      }
    } else {
      Future.successful(Map.empty)
    }

    deferred.map {
      case DeferTotalFiles(consignmentId)  => context.fileService.fileCount(consignmentId)
      case DeferFileSizeSum(consignmentId) => context.fileMetadataService.getSumOfFileSizes(consignmentId)
      case DeferFileChecksProgress(consignmentId) =>
        context.fileStatusService.getConsignmentFileProgress(consignmentId)
      case DeferParentFolder(consignmentId)        => context.consignmentService.getConsignmentParentFolder(consignmentId)
      case DeferParentFolderId(consignmentId)      => context.fileService.getConsignmentParentFolderId(consignmentId)
      case DeferConsignmentStatuses(consignmentId) => consignmentStatusFuture.map(_.getOrElse(consignmentId, List.empty))
      case DeferFiles(consignmentId, fileFilters: Option[FileFilters], queriedFileFields) =>
        context.fileService.getFileMetadata(consignmentId, fileFilters, queriedFileFields)
      case DeferPaginatedFiles(consignmentId, paginationInput, queriedFileFields) =>
        context.fileService.getPaginatedFiles(consignmentId, paginationInput, queriedFileFields)
      case DeferChecksSucceeded(consignmentId)             => context.fileStatusService.allChecksSucceeded(consignmentId)
      case DeferClosedRecords(consignmentId)               => context.consignmentService.totalClosedRecords(consignmentId)
      case DeferConsignmentMetadata(consignmentId, filter) => context.consignmentMetadataService.getConsignmentMetadata(consignmentId, filter)
      case other                                           => throw UnsupportedDeferError(other)
    }
  }
  // scalastyle:on cyclomatic.complexity
}

case class DeferTotalFiles(consignmentId: UUID) extends Deferred[Int]
case class DeferFileSizeSum(consignmentId: UUID) extends Deferred[Long]
case class DeferFileChecksProgress(consignmentId: UUID) extends Deferred[FileChecks]
case class DeferParentFolder(consignmentId: UUID) extends Deferred[Option[String]]
case class DeferParentFolderId(consignmentId: UUID) extends Deferred[Option[UUID]]
case class DeferFiles(consignmentId: UUID, fileFilters: Option[FileFilters] = None, queriedFileFields: QueriedFileFields) extends Deferred[List[File]]
case class DeferPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput], queriedFileFields: QueriedFileFields) extends Deferred[TDRConnection[File]]
case class DeferClosedRecords(consignmentId: UUID) extends Deferred[Int]

case class DeferChecksSucceeded(consignmentId: UUID) extends Deferred[Boolean]

case class DeferConsignmentStatuses(consignmentId: UUID) extends Deferred[List[ConsignmentStatus]]
case class DeferConsignmentMetadata(consignmentId: UUID, filter: Option[ConsignmentMetadataFilter]) extends Deferred[List[ConsignmentMetadata]]

case class QueriedFileFields(
    originalFilePath: Boolean = false,
    antivirusMetadata: Boolean = false,
    ffidMetadata: Boolean = false,
    fileStatus: Boolean = false,
    fileStatuses: Boolean = false
)
