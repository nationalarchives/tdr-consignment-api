package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.MetadatareviewlogRow
import uk.gov.nationalarchives.tdr.api.db.repository.MetadataReviewLogRepository
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.MetadataReviewLog
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.TimestampUtils

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MetadataReviewService(
    metadataReviewLogRepository: MetadataReviewLogRepository
)(implicit val executionContext: ExecutionContext) {

  private def toMetadataReviewLog(row: MetadatareviewlogRow): MetadataReviewLog = {
    MetadataReviewLog(
      row.metadatareviewlogid,
      row.consignmentid,
      row.userid,
      row.action,
      row.eventtime.toZonedDateTime
    )
  }

  def getMetadataReviewDetails(consignmentId: UUID): Future[Seq[MetadataReviewLog]] = {
    metadataReviewLogRepository.getEntriesByConsignmentId(consignmentId).map(_.map(toMetadataReviewLog))
  }
}
