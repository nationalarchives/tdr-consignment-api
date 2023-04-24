package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.AvmetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository.AntivirusMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.{AddAntivirusMetadataInput, AntivirusMetadata}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class AntivirusMetadataService(antivirusMetadataRepository: AntivirusMetadataRepository, uuidSource: UUIDSource, timeSource: TimeSource)(implicit
    val executionContext: ExecutionContext
) {

  def addAntivirusMetadata(input: AddAntivirusMetadataInput): Future[List[AntivirusMetadata]] = {
    val inputRows = input.antivirusMetadata
      .map(values => {
        val inputRow =
          AvmetadataRow(values.fileId, values.software, values.softwareVersion, values.databaseVersion, values.result, Timestamp.from(Instant.ofEpochMilli(values.datetime)))
        inputRow
      })
    antivirusMetadataRepository.addAntivirusMetadata(inputRows).map(rowsToAntivirusMetadata)
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
