package uk.gov.nationalarchives.tdr.api.service

import net.logstash.logback.argument.StructuredArguments.value

import java.sql.Timestamp
import java.time.Instant
import uk.gov.nationalarchives.Tables.{AvmetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.AntivirusMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.{AddAntivirusMetadataInput, AntivirusMetadata}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.Logger

class AntivirusMetadataService(antivirusMetadataRepository: AntivirusMetadataRepository, uuidSource: UUIDSource, timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {

  val logger: Logger = Logger("AntivirusMetadataService")

  def addAntivirusMetadata(input: AddAntivirusMetadataInput): Future[AntivirusMetadata] = {

    val inputRow = AvmetadataRow(
      input.fileId,
      input.software,
      input.softwareVersion,
      input.databaseVersion,
      input.result,
      Timestamp.from(Instant.ofEpochMilli(input.datetime)))
    val fileStatusValue = input.result match {
      case "" => Success
      case _ => VirusDetected
    }
    val fileStatusRow = FilestatusRow(uuidSource.uuid, input.fileId, Antivirus, fileStatusValue, Timestamp.from(timeSource.now))

    logger.info("File check {} for fileId {} completed with status {}",
      value("fileCheck", "antivirus"),
      value("fileId", input.fileId),
      value("fileCheckStatus", fileStatusValue))

    antivirusMetadataRepository.addAntivirusMetadata(inputRow, fileStatusRow).map(rowToAntivirusMetadata)
  }

  private def rowToAntivirusMetadata(row: AvmetadataRow): AntivirusMetadata = {
    AntivirusMetadata(
      row.fileid,
      row.software,
      row.softwareversion,
      row.databaseversion,
      row.result,
      row.datetime.getTime
    )
  }

  def getAntivirusMetadata(consignmentId: UUID): Future[List[AntivirusMetadata]] = {
    antivirusMetadataRepository.getAntivirusMetadata(consignmentId)
      .map(r => r.map(rowToAntivirusMetadata).toList)
  }
}
