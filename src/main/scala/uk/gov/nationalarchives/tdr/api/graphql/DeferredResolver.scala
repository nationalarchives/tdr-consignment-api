package uk.gov.nationalarchives.tdr.api.graphql

import java.util.UUID
import sangria.execution.deferred.{Deferred, UnsupportedDeferError}
import uk.gov.nationalarchives.tdr.api.db.repository.FileFilters
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{CurrentStatus, FileChecks, PaginationInput, TransferringBody}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.ConsignmentStatus
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.File
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection

import scala.concurrent.{ExecutionContext, Future}

class DeferredResolver extends sangria.execution.deferred.DeferredResolver[ConsignmentApiContext] {
  // We may at some point need to do authorisation in this method. There is a ensurePermissions method which needs to be called before returning data.
  // scalastyle:off cyclomatic.complexity
  override def resolve(deferred: Vector[Deferred[Any]], context: ConsignmentApiContext, queryState: Any)(implicit ec: ExecutionContext): Vector[Future[Any]] = {
    deferred.map {
      case DeferTotalFiles(consignmentId)  => context.fileService.fileCount(consignmentId)
      case DeferFileSizeSum(consignmentId) => context.fileMetadataService.getSumOfFileSizes(consignmentId)
      case DeferFileChecksProgress(consignmentId) =>
        context.consignmentService.getConsignmentFileProgress(consignmentId)
      case DeferParentFolder(consignmentId)                       => context.consignmentService.getConsignmentParentFolder(consignmentId)
      case DeferParentFolderId(consignmentId)                     => context.fileService.getConsignmentParentFolderId(consignmentId)
      case DeferConsignmentSeries(consignmentId)                  => context.consignmentService.getSeriesOfConsignment(consignmentId)
      case DeferConsignmentBody(consignmentId)                    => context.consignmentService.getTransferringBodyOfConsignment(consignmentId)
      case DeferCurrentConsignmentStatus(consignmentId)           => context.consignmentStatusService.getConsignmentStatus(consignmentId)
      case DeferConsignmentStatuses(consignmentId, queriedFields) => context.consignmentStatusService.getConsignmentStatuses(consignmentId, queriedFields)
      case DeferFiles(consignmentId, fileFilters: Option[FileFilters], queriedFileFields) =>
        context.fileService.getFileMetadata(consignmentId, fileFilters, queriedFileFields)
      case DeferPaginatedFiles(consignmentId, paginationInput, queriedFileFields) =>
        context.fileService.getPaginatedFiles(consignmentId, paginationInput, queriedFileFields)
      case DeferChecksSucceeded(consignmentId) => context.fileStatusService.allChecksSucceeded(consignmentId)
      case other                               => throw UnsupportedDeferError(other)
    }
  }
  // scalastyle:on cyclomatic.complexity
}

case class DeferTotalFiles(consignmentId: UUID) extends Deferred[Int]
case class DeferFileSizeSum(consignmentId: UUID) extends Deferred[Int]
case class DeferFileChecksProgress(consignmentId: UUID) extends Deferred[FileChecks]
case class DeferParentFolder(consignmentId: UUID) extends Deferred[Option[String]]
case class DeferParentFolderId(consignmentId: UUID) extends Deferred[Option[UUID]]
case class DeferConsignmentSeries(consignmentId: UUID) extends Deferred[Option[Series]]
case class DeferConsignmentBody(consignmentId: UUID) extends Deferred[TransferringBody]
case class DeferFiles(consignmentId: UUID, fileFilters: Option[FileFilters] = None, queriedFileFields: QueriedFileFields) extends Deferred[List[File]]
case class DeferPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput], queriedFileFields: QueriedFileFields) extends Deferred[TDRConnection[File]]
case class DeferCurrentConsignmentStatus(consignmentId: UUID) extends Deferred[CurrentStatus]
case class DeferChecksSucceeded(consignmentId: UUID) extends Deferred[Boolean]

case class DeferConsignmentStatuses(consignmentId: UUID, queriedFields: QueriedConsignmentFields) extends Deferred[List[ConsignmentStatus]]

case class QueriedFileFields(
    originalFilePath: Boolean = false,
    antivirusMetadata: Boolean = false,
    ffidMetadata: Boolean = false,
    fileStatus: Boolean = false,
    fileStatuses: Boolean = false
)

case class QueriedConsignmentFields(consignmentStatuses: Boolean = false)
