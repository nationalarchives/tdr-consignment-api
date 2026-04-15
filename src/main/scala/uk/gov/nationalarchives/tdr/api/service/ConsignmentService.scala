package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config
import uk.gov.nationalarchives.Tables.{ConsignmentRow, ConsignmentstatusRow, MetadatareviewlogRow}
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentReference => ConsignmentReferenceOrderField}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentReference
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.ConsignmentTypeHelper
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils
import uk.gov.nationalarchives.tdr.common.utils.statuses.MetadataReviewLogAction.MetadataReviewLogAction
import uk.gov.nationalarchives.tdr.common.utils.statuses.MetadataReviewStatus
import uk.gov.nationalarchives.tdr.keycloak.Token

import java.sql.Timestamp
import java.time.{LocalDate, ZoneOffset}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class ConsignmentService(
    consignmentRepository: ConsignmentRepository,
    consignmentStatusRepository: ConsignmentStatusRepository,
    seriesRepository: SeriesRepository,
    fileMetadataRepository: FileMetadataRepository,
    transferringBodyService: TransferringBodyService,
    metadataReviewLogRepository: MetadataReviewLogRepository,
    timeSource: TimeSource,
    uuidSource: UUIDSource,
    config: Config
)(implicit val executionContext: ExecutionContext) {

  val maxLimit: Int = config.getInt("pagination.consignmentsMaxLimit")

  // Review status sort ordering: Requested > Rejected > Approved > Completed
  private val reviewStatusOrder: Map[String, Int] = Map(
    MetadataReviewStatus.Requested.value -> 0,
    MetadataReviewStatus.Rejected.value -> 1,
    MetadataReviewStatus.Approved.value -> 2,
    MetadataReviewStatus.Completed.value -> 3
  )

  def startUpload(startUploadInput: StartUploadInput): Future[String] = {
    consignmentStatusRepository
      .getConsignmentStatus(startUploadInput.consignmentId)
      .flatMap(status => {
        val uploadStatus = status.find(s => s.statustype == Upload)
        if (uploadStatus.isDefined) {
          throw ConsignmentStateException(s"Existing consignment upload status is '${uploadStatus.get.value}', so cannot start new upload")
        }
        val now = Timestamp.from(timeSource.now)
        val consignmentStatusUploadRow = ConsignmentstatusRow(uuidSource.uuid, startUploadInput.consignmentId, Upload, InProgress, now)
        val consignmentStatusClientChecksRow = ConsignmentstatusRow(uuidSource.uuid, startUploadInput.consignmentId, ClientChecks, InProgress, now)
        consignmentRepository.addUploadDetails(
          startUploadInput,
          List(consignmentStatusUploadRow, consignmentStatusClientChecksRow)
        )
      })
  }

  def totalClosedRecords(consignmentId: UUID): Future[Int] = {
    fileMetadataRepository.totalClosedRecords(consignmentId)
  }

  def updateTransferInitiated(consignmentId: UUID, userId: UUID): Future[Int] = {
    for {
      updateTransferInitiatedStatus <- consignmentRepository.updateTransferInitiated(consignmentId, userId, Timestamp.from(timeSource.now))
      consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "Export", InProgress, Timestamp.from(timeSource.now))
      _ <- consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
    } yield updateTransferInitiatedStatus
  }

  def updateExportData(exportDataInput: UpdateExportDataInput): Future[Int] = {
    consignmentRepository.updateExportData(exportDataInput)
  }

  def addConsignment(addConsignmentInput: AddConsignmentInput, token: Token): Future[Consignment] = {
    val now = timeSource.now
    val yearNow = LocalDate.from(now.atOffset(ZoneOffset.UTC)).getYear
    val timestampNow = Timestamp.from(now)
    val consignmentType: String = addConsignmentInput.consignmentType.validateType

    val userBodyCode = token.transferringBody.getOrElse(throw InputDataException(s"No transferring body in user token for user '${token.userId}'"))
    val seriesId = addConsignmentInput.seriesid

    for {
      sequence <- consignmentRepository.getNextConsignmentSequence
      body <- transferringBodyService.getBodyByCode(userBodyCode)
      seriesName <- getSeriesName(seriesId)
      consignmentRef = ConsignmentReference.createConsignmentReference(yearNow, sequence)
      consignmentId = uuidSource.uuid
      consignmentRow = ConsignmentRow(
        consignmentId,
        seriesId,
        token.userId,
        timestampNow,
        consignmentsequence = sequence,
        consignmentreference = consignmentRef,
        consignmenttype = consignmentType,
        bodyid = body.bodyId,
        seriesname = seriesName,
        transferringbodyname = Some(body.name),
        transferringbodytdrcode = Some(body.tdrCode)
      )
      consignment <- consignmentRepository.addConsignment(consignmentRow).map(row => convertRowToConsignment(row))
    } yield consignment
  }

  def getConsignment(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignments = consignmentRepository.getConsignment(consignmentId)
    consignments.map(rows => rows.headOption.map(row => convertRowToConsignment(row)))
  }

  def getConsignments(
      limit: Int,
      currentCursor: Option[String],
      consignmentFilters: Option[ConsignmentFilters] = None,
      currentPage: Option[Int] = None,
      consignmentOrderBy: Option[ConsignmentOrderBy] = None
  ): Future[PaginatedConsignments] = {
    val maxConsignments: Int = min(limit, maxLimit)
    val orderBy = consignmentOrderBy.getOrElse(ConsignmentOrderBy(ConsignmentReferenceOrderField, Descending))
    for {
      response <- consignmentRepository.getConsignments(maxConsignments, currentCursor, currentPage, consignmentFilters, orderBy)
      hasNextPage = response.nonEmpty
      paginatedConsignments = convertToEdges(response, orderBy.consignmentOrderField)
      lastCursor = if (hasNextPage) Some(orderBy.consignmentOrderField.cursorFn(response.last)) else None
    } yield PaginatedConsignments(lastCursor, paginatedConsignments)
  }

  def getConsignmentsForMetadataReview: Future[Seq[Consignment]] = {
    consignmentRepository.getConsignmentsForMetadataReview.map(rows => rows.map(row => convertRowToConsignment(row)))
  }

  def getConsignmentReviewDetails(statusFilter: Option[String]): Future[Seq[ConsignmentReviewDetails]] = {
    for {
      consignmentRows <- consignmentRepository.getConsignmentsWithMetadataReviewStatus
      consignmentIds = consignmentRows.map(_.consignmentid)
      logEntries <- metadataReviewLogRepository.getEntriesByConsignmentIds(consignmentIds)
    } yield {
      val latestLogByConsignment = latestLogPerConsignment(logEntries)
      val details = buildReviewDetails(consignmentRows, latestLogByConsignment)
      val filtered = filterByStatus(details, statusFilter)
      sortByReviewStatus(filtered)
    }
  }

  def getConsignmentForMetadataReview(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignment = consignmentRepository.getConsignmentForMetadataReview(consignmentId)
    consignment.map(rows => rows.map(row => convertRowToConsignment(row)).headOption)
  }

  def updateSeriesOfConsignment(updateConsignmentSeriesIdInput: UpdateConsignmentSeriesIdInput): Future[Int] = {
    for {
      seriesName <- getSeriesName(Some(updateConsignmentSeriesIdInput.seriesId))
      result <- consignmentRepository.updateSeriesOfConsignment(updateConsignmentSeriesIdInput, seriesName)
      seriesStatus = if (result == 1) Completed else Failed
      _ <- consignmentStatusRepository.updateConsignmentStatus(updateConsignmentSeriesIdInput.consignmentId, "Series", seriesStatus, Timestamp.from(timeSource.now))
    } yield result
  }

  def consignmentHasFiles(consignmentId: UUID): Future[Boolean] = {
    consignmentRepository.consignmentHasFiles(consignmentId)
  }

  def getConsignmentParentFolder(consignmentId: UUID): Future[Option[String]] = {
    consignmentRepository.getParentFolder(consignmentId)
  }

  def getTotalPages(limit: Int, consignmentFilters: Option[ConsignmentFilters]): Future[Int] = {
    val maxConsignmentsLimit: Int = min(limit, maxLimit)
    consignmentRepository.getTotalConsignments(consignmentFilters).map(totalItems => Math.ceil(totalItems.toDouble / maxConsignmentsLimit.toDouble).toInt)
  }

  def updateMetadataSchemaLibraryVersion(updateMetadataSchemaLibraryVersionInput: UpdateMetadataSchemaLibraryVersionInput): Future[Int] = {
    consignmentRepository.updateMetadataSchemaLibraryVersion(updateMetadataSchemaLibraryVersionInput)
  }

  def updateClientSideDraftMetadataFileName(input: UpdateClientSideDraftMetadataFileNameInput): Future[Int] = {
    consignmentRepository.updateClientSideDraftMetadataFileName(input)
  }

  def updateParentFolder(input: UpdateParentFolderInput): Future[Int] = {
    consignmentRepository.updateParentFolder(input.consignmentId, input.parentFolder)
  }

  private def latestLogPerConsignment(logEntries: Seq[MetadatareviewlogRow]): Map[UUID, MetadatareviewlogRow] = {
    logEntries
      .groupBy(_.consignmentid)
      .view
      .mapValues(_.maxBy(_.eventtime.getTime))
      .toMap
  }

  private def buildReviewDetails(consignmentRows: Seq[ConsignmentRow], latestLogByConsignment: Map[UUID, MetadatareviewlogRow]): Seq[ConsignmentReviewDetails] = {
    consignmentRows.flatMap { row =>
      latestLogByConsignment.get(row.consignmentid).map { latestLog =>
        ConsignmentReviewDetails(
          consignmentReference = row.consignmentreference,
          reviewStatus = MetadataReviewLogAction(latestLog.action).reviewStatus.value,
          transferringBodyName = row.transferringbodyname,
          seriesName = row.seriesname,
          lastUpdated = latestLog.eventtime.toZonedDateTime
        )
      }
    }
  }

  private def filterByStatus(details: Seq[ConsignmentReviewDetails], statusFilter: Option[String]): Seq[ConsignmentReviewDetails] = {
    statusFilter match {
      case None         => details
      case Some(status) => details.filter(_.reviewStatus == status)
    }
  }

  private def sortByReviewStatus(details: Seq[ConsignmentReviewDetails]): Seq[ConsignmentReviewDetails] = {
    details.sortBy(d => (reviewStatusOrder.getOrElse(d.reviewStatus, Int.MaxValue), -d.lastUpdated.toInstant.toEpochMilli))
  }

  private def convertRowToConsignment(row: ConsignmentRow): Consignment = {
    Consignment(
      row.consignmentid,
      row.userid,
      row.seriesid,
      row.datetime.toZonedDateTime,
      row.transferinitiateddatetime.map(ts => ts.toZonedDateTime),
      row.exportdatetime.map(ts => ts.toZonedDateTime),
      row.exportlocation,
      row.consignmentreference,
      row.consignmenttype,
      row.bodyid,
      row.includetoplevelfolder,
      row.seriesname,
      row.transferringbodyname,
      row.transferringbodytdrcode,
      row.metadataschemalibraryversion,
      row.clientsidedraftmetadatafilename
    )
  }

  private def convertToEdges(consignmentRows: Seq[ConsignmentRow], consignmentOrderField: ConsignmentOrderField): Seq[ConsignmentEdge] = {
    consignmentRows
      .map(cr => ConsignmentEdge(convertRowToConsignment(cr), consignmentOrderField.cursorFn(cr)))
  }

  private def getSeriesName(seriesId: Option[UUID]): Future[Option[String]] = {
    if (seriesId.isDefined) {
      seriesRepository.getSeries(seriesId.get).map(_.headOption.map(_.name))
    } else {
      Future(None)
    }
  }
}

case class PaginatedConsignments(lastCursor: Option[String], consignmentEdges: Seq[ConsignmentEdge])
