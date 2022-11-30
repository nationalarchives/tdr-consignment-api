package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{AvmetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.AntivirusMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.{AddAntivirusMetadataInput, AddAntivirusMetadataInputValues, AntivirusMetadata}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Antivirus, Success, VirusDetected}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataService(antivirusMetadataRepository: AntivirusMetadataRepository, uuidSource: UUIDSource, timeSource: TimeSource)(implicit
    val executionContext: ExecutionContext
) {

  @deprecated("Use addAntivirusMetadata(input: AddAntivirusMetadataInput): Future[List[AntivirusMetadata]] instead")
  def addAntivirusMetadata(values: AddAntivirusMetadataInputValues): Future[AntivirusMetadata] =
    addAntivirusMetadata(AddAntivirusMetadataInput(values :: Nil)).map(_.head)

  def addAntivirusMetadata(input: AddAntivirusMetadataInput): Future[List[AntivirusMetadata]] = {
    val (inputRows, fileStatusRows) = input.antivirusMetadata
      .map(values => {
        val inputRow =
          AvmetadataRow(values.fileId, values.software, values.softwareVersion, values.databaseVersion, values.result, Timestamp.from(Instant.ofEpochMilli(values.datetime)))
        val fileStatusValue = values.result match {
          case "" => Success
          case _  => VirusDetected
        }
        val fileStatusRow = FilestatusRow(uuidSource.uuid, values.fileId, Antivirus, fileStatusValue, Timestamp.from(timeSource.now))
        (inputRow, fileStatusRow)
      })
      .unzip
    antivirusMetadataRepository.addAntivirusMetadata(inputRows, fileStatusRows).map(rowsToAntivirusMetadata)
  }

  private def rowsToAntivirusMetadata(rows: List[AvmetadataRow]): List[AntivirusMetadata] = {
    rows.map(row => {
      AntivirusMetadata(
        row.fileid,
        row.software,
        row.softwareversion,
        row.databaseversion,
        row.result,
        row.datetime.getTime
      )
    })
  }

  def getAntivirusMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[List[AntivirusMetadata]] = {
    antivirusMetadataRepository
      .getAntivirusMetadata(consignmentId, selectedFileIds)
      .map(_.toList)
      .map(rowsToAntivirusMetadata)
  }
}
