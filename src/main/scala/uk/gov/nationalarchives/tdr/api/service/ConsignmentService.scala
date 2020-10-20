package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.util.UUID

import uk.gov.nationalarchives.Tables.ConsignmentRow
import uk.gov.nationalarchives.tdr.api.db.repository.{ConsignmentRepository, FFIDMetadataRepository, FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields._

import scala.concurrent.{ExecutionContext, Future}


class ConsignmentService(
                          consignmentRepository: ConsignmentRepository,
                          fileMetadataRepository: FileMetadataRepository,
                          fileRepository: FileRepository,
                          ffidMetadataRepository: FFIDMetadataRepository,
                          timeSource: TimeSource,
                          uuidSource: UUIDSource
                        )(implicit val executionContext: ExecutionContext) {

  def addConsignment(addConsignmentInput: AddConsignmentInput, userId: Option[UUID]): Future[Consignment] = {
      val consignmentRow = ConsignmentRow(uuidSource.uuid, addConsignmentInput.seriesid, userId.get, Timestamp.from(timeSource.now))
      consignmentRepository.addConsignment(consignmentRow).map(row => Consignment(Some(row.consignmentid), row.userid, row.seriesid))
    }

  def getConsignment(consignmentId: UUID): Future[Option[Consignment]] = {
    val consignments = consignmentRepository.getConsignment(consignmentId)
    consignments.map(rows => rows.headOption.map(row => Consignment(Some(row.consignmentid), row.userid, row.seriesid)))
  }

  def consignmentHasFiles(consignmentId: UUID): Future[Boolean] = {
    consignmentRepository.consignmentHasFiles(consignmentId)
  }

  def getConsignmentFileProgress(consignmentId: UUID): Future[FileChecks] = {
    for {
      processed <- fileRepository.countProcessedAvMetadataInConsignment(consignmentId)
      checksum <- fileMetadataRepository.countProcessedChecksumInConsignment(consignmentId)
      fileFormatId <- ffidMetadataRepository.countProcessedFfidMetadata(consignmentId)
    } yield FileChecks(AntivirusProgress(processed), ChecksumProgress(checksum), FFIDProgress(fileFormatId))
  }

  def getConsignmentParentFolder(consignmentId: UUID): Future[String] = {
    consignmentRepository.getParentFolder(consignmentId).map(name => name.getOrElse("Parent folder not found"))
  }
}
