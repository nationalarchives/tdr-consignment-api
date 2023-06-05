package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(
    ffidMetadataRepository: FFIDMetadataRepository,
    matchesRepository: FFIDMetadataMatchesRepository,
    timeSource: TimeSource,
    uuidSource: UUIDSource
)(implicit val executionContext: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FFIDMetadataService"))

  def addFFIDMetadata(ffidMetadataInput: FFIDMetadataInput): Future[List[FFIDMetadata]] = {
    val (metadataRows, matchRows) = ffidMetadataInput.metadataInputValues
      .map(ffidMetadata => {
        if (ffidMetadata.matches.isEmpty) {
          throw InputDataException(s"No ffid matches for file ${ffidMetadata.fileId}")
        }

        val metadataRow = FfidmetadataRow(
          uuidSource.uuid,
          ffidMetadata.fileId,
          ffidMetadata.software,
          ffidMetadata.softwareVersion,
          Timestamp.from(timeSource.now),
          ffidMetadata.binarySignatureFileVersion,
          ffidMetadata.containerSignatureFileVersion,
          ffidMetadata.method
        )

        val matchRows = ffidMetadata.matches.map(m => FfidmetadatamatchesRow(metadataRow.ffidmetadataid, m.extension, m.identificationBasis, m.puid))

        (metadataRow, matchRows)
      })
      .unzip

    val metadataUpdate = for {
      ffidMetadata <- ffidMetadataRepository.addFFIDMetadata(metadataRows)
      matches <- matchesRepository.addFFIDMetadataMatches(matchRows.flatten)
    } yield {
      val matchesMap = matches.groupBy(_.ffidmetadataid)
      ffidMetadata.map(ffid => rowToFFIDMetadata(ffid, matchesMap(ffid.ffidmetadataid)))
    }
    metadataUpdate.recover(err => {
      throw InputDataException(err.getMessage)
    })
  }

  def getFFIDMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[List[FFIDMetadata]] = {
    ffidMetadataRepository.getFFIDMetadata(consignmentId, selectedFileIds).map { ffidMetadataAndMatchesRows =>
      val ffidMetadataAndMatches: Map[FfidmetadataRow, Seq[FfidmetadatamatchesRow]] = {
        ffidMetadataAndMatchesRows.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      }
      ffidMetadataAndMatches.map { case (metadata, matches) =>
        rowToFFIDMetadata(metadata, matches)
      }.toList
    }
  }

  private def rowToFFIDMetadata(ffidMetadataRow: nationalarchives.Tables.FfidmetadataRow, ffidMetadataMatchesRow: Seq[FfidmetadatamatchesRow]): FFIDMetadata = {
    FFIDMetadata(
      ffidMetadataRow.fileid,
      ffidMetadataRow.software,
      ffidMetadataRow.softwareversion,
      ffidMetadataRow.binarysignaturefileversion,
      ffidMetadataRow.containersignaturefileversion,
      ffidMetadataRow.method,
      ffidMetadataMatchesRow.map(r => FFIDMetadataMatches(r.extension, r.identificationbasis, r.puid)).toList,
      ffidMetadataRow.datetime.getTime
    )
  }
}
