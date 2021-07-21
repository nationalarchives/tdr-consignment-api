package uk.gov.nationalarchives.tdr.api.service

import java.sql.{SQLException, Timestamp}
import java.util.UUID
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ClientFileMetadataFields.ClientFileMetadata
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.Tables.FilemetadataRow
import scala.concurrent.{ExecutionContext, Future}

class ClientFileMetadataService(fileMetadataRepository: FileMetadataRepository)
                               (implicit val executionContext: ExecutionContext) {

  def getClientFileMetadata(fileId: UUID): Future[ClientFileMetadata] = {
    fileMetadataRepository.getFileMetadata(fileId, clientSideProperties: _*)
      .map(rows => convertToResponse(fileId, rows))
      .recover {
        case nse: NoSuchElementException => throw InputDataException(s"Could not find client metadata for file $fileId", Some(nse))
        case e: SQLException => throw InputDataException(e.getLocalizedMessage, Some(e))
      }
  }

  private def convertToResponse(fileId: UUID, rows: Seq[FilemetadataRow]): ClientFileMetadata = {
    val propertyNameToValue = rows.map(row => row.propertyname -> row.value).toMap
    ClientFileMetadata(fileId,
      propertyNameToValue.get(ClientSideOriginalFilepath),
      propertyNameToValue.get(SHA256ClientSideChecksum),
      Some("SHA256"),
      Timestamp.valueOf(propertyNameToValue(ClientSideFileLastModifiedDate)).getTime,
      propertyNameToValue.get(ClientSideFileSize).map(_.toLong)
    )
  }
}
