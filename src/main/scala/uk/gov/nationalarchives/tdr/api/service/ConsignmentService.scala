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
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.{ConsignmentTypeHelper, standard}
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
    consignmentStatusRepository.getConsignmentStatus(startUploadInput.consignmentId).flatMap(status => {
      val uploadStatus = status.find(s => s.statustype == "Upload")
      if(uploadStatus.isDefined) {
        throw ConsignmentStateException(s"Existing consignment upload status is '${uploadStatus.get.value}', so cannot start new upload")
      }
      val now = Timestamp.from(timeSource.now)
      val consignmentStatusRow = ConsignmentstatusRow(uuidSource.uuid, startUploadInput.consignmentId, "Upload", "InProgress", now)
      consignmentRepository.addParentFolder(startUploadInput.consignmentId, startUploadInput.parentFolder, consignmentStatusRow)
    })
  }

  def updateTransferInitiated(consignmentId: UUID, userId: UUID): Future[Int] = {
    consignmentRepository.updateTransferInitiated(consignmentId, userId, Timestamp.from(timeSource.now))
  }

  def updateExportLocation(exportLocationInput: UpdateExportLocationInput): Future[Int] = {
    consignmentRepository.updateExportLocation(exportLocationInput)
  }

  def addConsignment(addConsignmentInput: AddConsignmentInput, token: Token): Future[Consignment] = {
    val now = timeSource.now
    val yearNow = LocalDate.from(now.atOffset(ZoneOffset.UTC)).getYear
    val consignmentType: String = addConsignmentInput.consignmentType.validateType

    val userBody = token.transferringBody.getOrElse(
      throw InputDataException(s"No transferring body in user token for user '${token.userId}'"))

    for {
      sequence <- consignmentRepository.getNextConsignmentSequence
      body <- transferringBodyService.getBodyByCode(userBody)
      consignmentRef = ConsignmentReference.createConsignmentReference(yearNow, sequence)
      consignmentRow = ConsignmentRow(
          uuidSource.uuid,
          addConsignmentInput.seriesid,
          token.userId,
          Timestamp.from(now),
          consignmentsequence = sequence,
          consignmentreference = consignmentRef,
          consignmenttype = consignmentType,
          bodyid = body.bodyId
        )
      consignment <- consignmentRepository.addConsignment(consignmentRow).map(
        row => convertRowToConsignment(row)
      )
    } yield consignment
  }

  def getConsignment(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignments = consignmentRepository.getConsignment(consignmentId)
    consignments.map(rows => rows.headOption.map(
      row => convertRowToConsignment(row)))
  }

  def getConsignments(limit: Int, currentCursor: Option[String]): Future[PaginatedConsignments] = {
    val maxConsignments: Int = min(limit, maxLimit)

    for {
      response <- consignmentRepository.getConsignments(maxConsignments, currentCursor)
      hasNextPage = response.nonEmpty
      lastCursor: Option[String] = if (hasNextPage) Some(response.last.consignmentreference) else None
      paginatedConsignments = convertToEdges(response)
    } yield PaginatedConsignments(lastCursor, paginatedConsignments)
  }

  def getSeriesOfConsignment(consignmentId: UUID): Future[Option[Series]] = {
    val consignment: Future[Seq[SeriesRow]] = consignmentRepository.getSeriesOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(
      series => Series(series.seriesid, series.bodyid, series.name, series.code, series.description)))
  }

  def getTransferringBodyOfConsignment(consignmentId: UUID): Future[Option[TransferringBody]] = {
    val consignment: Future[Seq[BodyRow]] = consignmentRepository.getTransferringBodyOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(
      transferringBody => TransferringBody(transferringBody.name, transferringBody.tdrcode)))
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
      row.consignmentreference,
      row.consignmenttype,
      row.bodyid)
  }

  private def convertToEdges(consignmentRows: Seq[ConsignmentRow]): Seq[ConsignmentEdge] = {
    consignmentRows.map(cr => convertRowToConsignment(cr))
      .map(c => ConsignmentEdge(c, c.consignmentReference))
  }
}

case class PaginatedConsignments(lastCursor: Option[String], consignmentEdges: Seq[ConsignmentEdge])
