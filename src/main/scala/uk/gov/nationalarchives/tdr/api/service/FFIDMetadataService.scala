package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables

import java.sql.{SQLException, Timestamp}
import java.util.UUID
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FFIDMetadataMatchesRepository, FFIDMetadataRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataMatches}

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(ffidMetadataRepository: FFIDMetadataRepository, matchesRepository: FFIDMetadataMatchesRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {
  def addFFIDMetadata(ffidMetadata: FFIDMetadataInput): Future[FFIDMetadata] = {
    val metadataRows = FfidmetadataRow(uuidSource.uuid, ffidMetadata.fileId,
      ffidMetadata.software,
      ffidMetadata.softwareVersion,
      Timestamp.from(timeSource.now),
      ffidMetadata.binarySignatureFileVersion,
      ffidMetadata.containerSignatureFileVersion,
      ffidMetadata.method)

    def addFFIDMetadataMatches(ffidmetadataid: UUID): Future[Seq[Tables.FfidmetadatamatchesRow]] = {
      val matchRows = ffidMetadata.matches.map(m => FfidmetadatamatchesRow(ffidmetadataid, m.extension, m.identificationBasis, m.puid))
      matchesRepository.addFFIDMetadataMatches(matchRows)

    }

    (for {
      ffidMetadataRow <- ffidMetadataRepository.addFFIDMetadata(metadataRows)
      ffidMetadataMatchesRow <- addFFIDMetadataMatches(ffidMetadataRow.ffidmetadataid)
    } yield {
      rowToFFIDMetadata(ffidMetadataRow, ffidMetadataMatchesRow)
    }).recover {
      case e: SQLException => throw InputDataException(e.getLocalizedMessage)
    }
  }

  def getFFIDMetadata(consignmentId: UUID): Future[List[FFIDMetadata]] = {
    ffidMetadataRepository.getFFIDMetadata(consignmentId).map {
      ffidMetadataAndMatchesRows =>
        val ffidMetadataAndMatches: Map[FfidmetadataRow, Seq[FfidmetadatamatchesRow]] = {
          ffidMetadataAndMatchesRows.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
        }
        ffidMetadataAndMatches.map {
          case (metadata, matches) => rowToFFIDMetadata(metadata, matches)
        }.toList
    }
  }

  private def rowToFFIDMetadata(ffidMetadataRow: nationalarchives.Tables.FfidmetadataRow, ffidMetadataMatchesRow: Seq[FfidmetadatamatchesRow]) = {
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
