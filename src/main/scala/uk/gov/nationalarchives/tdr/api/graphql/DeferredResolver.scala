package uk.gov.nationalarchives.tdr.api.graphql

import java.util.UUID

import sangria.execution.deferred.{Deferred, UnsupportedDeferError}
import sangria.relay.DefaultConnection
import uk.gov.nationalarchives.tdr.api.db.repository.FileFilters
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{CurrentStatus, FileChecks, PaginationInput, TransferringBody}
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.File

import scala.concurrent.{ExecutionContext, Future}

class DeferredResolver extends sangria.execution.deferred.DeferredResolver[ConsignmentApiContext] {
  // We may at some point need to do authorisation in this method. There is a ensurePermissions method which needs to be called before returning data.
  override def resolve(deferred: Vector[Deferred[Any]], context: ConsignmentApiContext, queryState: Any)(implicit ec: ExecutionContext): Vector[Future[Any]] = {
    deferred.map {
      case DeferTotalFiles(consignmentId) => context.fileService.fileCount(consignmentId)
      case DeferFileChecksProgress(consignmentId) =>
        context.consignmentService.getConsignmentFileProgress(consignmentId)
      case DeferParentFolder(consignmentId) => context.consignmentService.getConsignmentParentFolder(consignmentId)
      case DeferConsignmentSeries(consignmentId) => context.consignmentService.getSeriesOfConsignment(consignmentId)
      case DeferConsignmentBody(consignmentId) => context.consignmentService.getTransferringBodyOfConsignment(consignmentId)
      case DeferCurrentConsignmentStatus(consignmentId) => context.consignmentStatusService.getConsignmentStatus(consignmentId)
      case DeferFiles(consignmentId, fileFilters: Option[FileFilters]) => context.fileService.getFileMetadata(consignmentId, fileFilters)
      case DeferPaginatedFiles(consignmentId, paginationInput, fileFilters) =>
        context.fileService.getPaginatedFiles(consignmentId, paginationInput, fileFilters)
      case DeferChecksSucceeded(consignmentId) => context.fileStatusService.allChecksSucceeded(consignmentId)
      case other => throw UnsupportedDeferError(other)
    }
  }
}

case class DeferTotalFiles(consignmentId: UUID) extends Deferred[Int]
case class DeferFileChecksProgress(consignmentId: UUID) extends Deferred[FileChecks]
case class DeferParentFolder(consignmentId: UUID) extends Deferred[Option[String]]
case class DeferConsignmentSeries(consignmentId: UUID) extends Deferred[Option[Series]]
case class DeferConsignmentBody(consignmentId: UUID) extends Deferred[TransferringBody]
case class DeferFiles(consignmentId: UUID, fileFilters: Option[FileFilters] = None) extends Deferred[List[File]]
case class DeferPaginatedFiles(consignmentId: UUID, paginationInput: Option[PaginationInput], fileFilters: Option[FileFilters] = None)
  extends Deferred[DefaultConnection[File]]
case class DeferCurrentConsignmentStatus(consignmentId: UUID) extends Deferred[CurrentStatus]
case class DeferChecksSucceeded(consignmentId: UUID) extends Deferred[Boolean]
