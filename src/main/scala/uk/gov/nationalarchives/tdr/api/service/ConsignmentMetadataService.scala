package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.ConsignmentMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentMetadata, ConsignmentMetadataFilter}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentMetadataFields.{
  AddOrUpdateConsignmentMetadata,
  AddOrUpdateConsignmentMetadataInput,
  ConsignmentMetadataWithConsignmentId
}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{AddOrUpdateBulkFileMetadataInput, AddOrUpdateFileMetadata, AddOrUpdateMetadata}
import uk.gov.nationalarchives.tdr.schema.generated.BaseSchema.legal_status
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentMetadataService(
    consignmentMetadataRepository: ConsignmentMetadataRepository,
    fileService: FileService,
    fileMetadataService: FileMetadataService,
    uuidSource: UUIDSource,
    timeSource: TimeSource
)(implicit val executionContext: ExecutionContext) {

  private val tdrDataLoadHeaderToPropertyMapper = ConfigUtils.loadConfiguration.propertyToOutputMapper("tdrDataLoadHeader")

  def addOrUpdateConsignmentMetadata(input: AddOrUpdateConsignmentMetadataInput, userId: UUID): Future[List[ConsignmentMetadataWithConsignmentId]] = {
    for {
      _ <- consignmentMetadataRepository.deleteConsignmentMetadata(input.consignmentId, input.consignmentMetadata.map(_.propertyName).toSet)
      rows <- consignmentMetadataRepository.addConsignmentMetadata(convertToConsignmentMetadataRows(input, userId))
      _ <- addLegalStatusFileMetadata(input.consignmentId, input.consignmentMetadata.find(_.propertyName == tdrDataLoadHeaderToPropertyMapper(legal_status)), userId)
    } yield {
      rows.map(row => ConsignmentMetadataWithConsignmentId(row.consignmentid, row.propertyname, row.value)).toList
    }
  }

  private def addLegalStatusFileMetadata(consignmentId: UUID, metadata: Option[AddOrUpdateConsignmentMetadata], userId: UUID): Future[Any] = {
    metadata match {
      case Some(md) =>
        fileService.getFileIds(consignmentId).flatMap { fileIds =>
          fileMetadataService.addOrUpdateBulkFileMetadata(createBulkFileMetadataInput(consignmentId, fileIds, md), userId)
        }
      case None => Future.successful()
    }
  }

  private def createBulkFileMetadataInput(consignmentId: UUID, fileIds: Seq[UUID], metadata: AddOrUpdateConsignmentMetadata): AddOrUpdateBulkFileMetadataInput = {
    AddOrUpdateBulkFileMetadataInput(
      consignmentId,
      fileIds.map(fileId => AddOrUpdateFileMetadata(fileId, Seq(AddOrUpdateMetadata(metadata.propertyName, metadata.value))))
    )
  }

  private def convertToConsignmentMetadataRows(input: AddOrUpdateConsignmentMetadataInput, userId: UUID): Seq[ConsignmentmetadataRow] = {
    val time = Timestamp.from(timeSource.now)
    val consignmentId = input.consignmentId
    input.consignmentMetadata.map(metadata => ConsignmentmetadataRow(uuidSource.uuid, consignmentId, metadata.propertyName, metadata.value, time, userId))
  }

  def getConsignmentMetadata(consignmentId: UUID, filter: Option[ConsignmentMetadataFilter]): Future[List[ConsignmentMetadata]] = {
    consignmentMetadataRepository.getConsignmentMetadata(consignmentId, filter).map { rows =>
      rows.map(row => ConsignmentMetadata(row.propertyname, row.value)).toList
    }
  }
}
