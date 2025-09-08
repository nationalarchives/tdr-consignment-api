package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentMetadataFields.{AddOrUpdateConsignmentMetadataInput, ConsignmentMetadataWithConsignmentId}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentMetadataService(
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  def addOrUpdateConsignmentMetadata(input: AddOrUpdateConsignmentMetadataInput, userId: UUID): Future[List[ConsignmentMetadataWithConsignmentId]] = {
    for {
      _ <- consignmentMetadataRepository.deleteConsignmentMetadata(input.consignmentId, input.consignmentMetadata.map(_.propertyName).toSet)
      rows <- consignmentMetadataRepository.addConsignmentMetadata(convertToConsignmentMetadataRows(input, userId))
    } yield {
      rows.map(row => ConsignmentMetadataWithConsignmentId(row.consignmentid, row.propertyname, row.value)).toList
    }
  }

  private def convertToConsignmentMetadataRows(input: AddOrUpdateConsignmentMetadataInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    input.consignmentMetadata.map(metadata => ConsignmentmetadataRow(uuidSource.uuid, consignmentId, metadata.propertyName, metadata.value, time, userId))
  }
}
