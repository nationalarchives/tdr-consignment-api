package uk.gov.nationalarchives.tdr.api.service

import net.logstash.logback.argument.StructuredArguments._

import java.sql.Timestamp
import java.time.Instant
import uk.gov.nationalarchives.Tables.{AvmetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.AntivirusMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.{AddAntivirusMetadataInput, AntivirusMetadata}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

class AntivirusMetadataService(antivirusMetadataRepository: AntivirusMetadataRepository, uuidSource: UUIDSource, timeSource: TimeSource)
                              (implicit val executionContext: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("AntivirusMetadataService"))

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
    val avMatch = if(fileStatusValue == VirusDetected) { s": ${input.result}" } else { "" }

    loggingUtils.logFileFormatStatus("antivirus", input.fileId, fileStatusValue + avMatch)

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
