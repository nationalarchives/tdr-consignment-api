package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.{LocalDate, ZoneOffset}
import java.util.UUID
import com.typesafe.config.Config
import uk.gov.nationalarchives.Tables.{BodyRow, ConsignmentRow, ConsignmentstatusRow, SeriesRow}
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields.Series
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentReference
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.ConsignmentTypeHelper
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils
import uk.gov.nationalarchives.tdr.keycloak.Token

import scala.concurrent.{ExecutionContext, Future}
import scala.math.min

class ConsignmentService(
    consignmentRepository: ConsignmentRepository,
    consignmentStatusRepository: ConsignmentStatusRepository,
    fileMetadataRepository: FileMetadataRepository,
    fileRepository: FileRepository,
    ffidMetadataRepository: FFIDMetadataRepository,
    transferringBodyService: TransferringBodyService,
    timeSource: TimeSource,
    uuidSource: UUIDSource,
    config: Config
)(implicit val executionContext: ExecutionContext) {

  val maxLimit: Int = config.getInt("pagination.consignmentsMaxLimit")

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
        consignmentRepository.addParentFolder(startUploadInput.consignmentId, startUploadInput.parentFolder, List(consignmentStatusUploadRow, consignmentStatusClientChecksRow))
      })
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

    val userBody = token.transferringBody.getOrElse(throw InputDataException(s"No transferring body in user token for user '${token.userId}'"))

    for {
      sequence <- consignmentRepository.getNextConsignmentSequence
      body <- transferringBodyService.getBodyByCode(userBody)
      consignmentRef = ConsignmentReference.createConsignmentReference(yearNow, sequence)
      consignmentId = uuidSource.uuid
      consignmentRow = ConsignmentRow(
        consignmentId,
        addConsignmentInput.seriesid,
        token.userId,
        timestampNow,
        consignmentsequence = sequence,
        consignmentreference = consignmentRef,
        consignmenttype = consignmentType,
        bodyid = body.bodyId
      )
      descriptiveMetadataStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, DescriptiveMetadata, "NotEntered", timestampNow, Option(timestampNow))
      closureMetadataStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, ClosureMetadata, "NotEntered", timestampNow, Option(timestampNow))
      consignment <- consignmentRepository.addConsignment(consignmentRow).map(row => convertRowToConsignment(row))
      _ <- consignmentStatusRepository.addConsignmentStatus(descriptiveMetadataStatusRow)
      _ <- consignmentStatusRepository.addConsignmentStatus(closureMetadataStatusRow)
    } yield consignment
  }

  def getConsignment(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignments = consignmentRepository.getConsignment(consignmentId)
    consignments.map(rows => rows.headOption.map(row => convertRowToConsignment(row)))
  }

  def getConsignments(limit: Int, currentCursor: Option[String], consignmentFilters: Option[ConsignmentFilters] = None): Future[PaginatedConsignments] = {
    val maxConsignments: Int = min(limit, maxLimit)

    for {
      response <- consignmentRepository.getConsignments(maxConsignments, currentCursor, consignmentFilters)
      hasNextPage = response.nonEmpty
      lastCursor: Option[String] = if (hasNextPage) Some(response.last.consignmentreference) else None
      paginatedConsignments = convertToEdges(response)
    } yield PaginatedConsignments(lastCursor, paginatedConsignments)
  }

  def getSeriesOfConsignment(consignmentId: UUID): Future[Option[Series]] = {
    val consignment: Future[Seq[SeriesRow]] = consignmentRepository.getSeriesOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(series => Series(series.seriesid, series.bodyid, series.name, series.code, series.description)))
  }

  def updateSeriesIdOfConsignment(updateConsignmentSeriesIdInput: UpdateConsignmentSeriesIdInput): Future[Int] = {
    addSeriesStatus(updateConsignmentSeriesIdInput.consignmentId)
    consignmentRepository.updateSeriesIdOfConsignment(updateConsignmentSeriesIdInput)
  }

  def addSeriesStatus(consignmentId: UUID): Future[ConsignmentstatusRow] = {
    val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, consignmentId, "Series", Completed, Timestamp.from(timeSource.now))
    consignmentStatusRepository.addConsignmentStatus(consignmentStatusRow)
  }

  def getTransferringBodyOfConsignment(consignmentId: UUID): Future[Option[TransferringBody]] = {
    val consignment: Future[Seq[BodyRow]] = consignmentRepository.getTransferringBodyOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(transferringBody => TransferringBody(transferringBody.name, transferringBody.tdrcode)))
  }

  def consignmentHasFiles(consignmentId: UUID): Future[Boolean] = {
    consignmentRepository.consignmentHasFiles(consignmentId)
  }

  def getConsignmentFileProgress(consignmentId: UUID): Future[FileChecks] = {
    for {
      avMetadataCount <- fileRepository.countProcessedAvMetadataInConsignment(consignmentId)
      checksumCount <- fileMetadataRepository.countProcessedChecksumInConsignment(consignmentId)
      fileFormatIdCount <- ffidMetadataRepository.countProcessedFfidMetadata(consignmentId)
    } yield FileChecks(AntivirusProgress(avMetadataCount), ChecksumProgress(checksumCount), FFIDProgress(fileFormatIdCount))
  }

  def getConsignmentParentFolder(consignmentId: UUID): Future[Option[String]] = {
    consignmentRepository.getParentFolder(consignmentId)
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
      row.bodyid
    )
  }

  private def convertToEdges(consignmentRows: Seq[ConsignmentRow]): Seq[ConsignmentEdge] = {
    consignmentRows
      .map(cr => convertRowToConsignment(cr))
      .map(c => ConsignmentEdge(c, c.consignmentReference))
  }
}

case class PaginatedConsignments(lastCursor: Option[String], consignmentEdges: Seq[ConsignmentEdge])
