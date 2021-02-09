package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.LocalDate
import java.util.{Calendar, UUID}
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

import uk.gov.nationalarchives.Tables.{BodyRow, ConsignmentRow, SeriesRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.SeriesFields.Series
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentReference

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentService(
                          consignmentRepository: ConsignmentRepository,
                          fileMetadataRepository: FileMetadataRepository,
                          fileRepository: FileRepository,
                          ffidMetadataRepository: FFIDMetadataRepository,
                          timeSource: TimeSource,
                          uuidSource: UUIDSource
                        )(implicit val executionContext: ExecutionContext) {

  def updateTransferInitiated(consignmentId: UUID, userId: UUID): Future[Int] = {
    consignmentRepository.updateTransferInitiated(consignmentId, userId, Timestamp.from(timeSource.now))
  }

  def updateExportLocation(exportLocationInput: UpdateExportLocationInput): Future[Int] = {
    consignmentRepository.updateExportLocation(exportLocationInput)
  }

  def addConsignment(addConsignmentInput: AddConsignmentInput, userId: UUID): Future[Consignment] = {
    val timeNow = timeSource.now
    val transferStartYearForReference: Int = LocalDate.from(timeSource.now).getYear
    val consignmentRow = ConsignmentRow(uuidSource.uuid, addConsignmentInput.seriesid, userId, Timestamp.from(timeSource.now))
    consignmentRepository.addConsignment(consignmentRow).map(
      row => convertRowToConsignment(row))
    val transferStartYearForReference: Int = calendar.get(Calendar.YEAR)
     consignmentRepository.getNextConsignmentSequence.flatMap(sequence => {
       val consignmentRef = ConsignmentReference.createConsignmentReference(transferStartYearForReference, sequence)
       val consignmentRow = ConsignmentRow(
         uuidSource.uuid,
         addConsignmentInput.seriesid,
         userId,
         Timestamp.from(timeNow),
         consignmentsequence = Option(sequence))
       consignmentRepository.addConsignment(consignmentRow).map(row => Consignment(row.consignmentid, row.userid, row.seriesid, Option(consignmentRef)))
     })
  }

  def getConsignment(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignments = consignmentRepository.getConsignment(consignmentId)
    consignments.map(rows => rows.headOption.map(
      row => convertRowToConsignment(row)))
  }

  def getSeriesOfConsignment(consignmentId: UUID): Future[Option[Series]] = {
    val consignment: Future[Seq[SeriesRow]] = consignmentRepository.getSeriesOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(
      series => Series(series.seriesid, series.bodyid, series.name, series.code, series.description)))
  }

  def getTransferringBodyOfConsignment(consignmentId: UUID): Future[Option[TransferringBody]] = {
    val consignment: Future[Seq[BodyRow]] = consignmentRepository.getTransferringBodyOfConsignment(consignmentId)
    consignment.map(rows => rows.headOption.map(
      transferringBody => TransferringBody(transferringBody.name, transferringBody.code)))
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
      row.exportdatetime.map(ts => ts.toZonedDateTime))
  }
}
