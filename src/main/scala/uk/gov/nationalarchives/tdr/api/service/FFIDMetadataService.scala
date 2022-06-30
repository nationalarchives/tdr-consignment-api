package uk.gov.nationalarchives.tdr.api.service

import java.sql.{SQLException, Timestamp}
import java.util.UUID
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FFIDMetadataMatchesRepository, FFIDMetadataRepository, FileRepository, PUIDRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(ffidMetadataRepository: FFIDMetadataRepository,
                          matchesRepository: FFIDMetadataMatchesRepository,
                          fileRepository: FileRepository,
                          puidRepository: PUIDRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FFIDMetadataService"))

  def addFFIDMetadata(ffidMetadata: FFIDMetadataInput): Future[FFIDMetadata] = {

    if (ffidMetadata.matches.isEmpty) {
      throw InputDataException(s"No ffid matches for file ${ffidMetadata.fileId}")
    }

    val metadataRow = FfidmetadataRow(uuidSource.uuid, ffidMetadata.fileId,
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
      fileStatusRows <- generateFileStatusRows(ffidMetadata)
      _ = loggingUtils.logFileFormatStatus("FFID", ffidMetadata.fileId, fileStatusRows.map(_.value).mkString(","))
      ffidMetadataRow <- ffidMetadataRepository.addFFIDMetadata(metadataRow, fileStatusRows)
      ffidMetadataMatchesRow <- addFFIDMetadataMatches(ffidMetadataRow.ffidmetadataid)
    } yield {
      rowToFFIDMetadata(ffidMetadataRow, ffidMetadataMatchesRow)
    }).recover {
      case e: SQLException => throw InputDataException(e.getLocalizedMessage)
    }
  }

  def getFFIDMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[List[FFIDMetadata]] = {
    ffidMetadataRepository.getFFIDMetadata(consignmentId, selectedFileIds).map {
      ffidMetadataAndMatchesRows =>
        val ffidMetadataAndMatches: Map[FfidmetadataRow, Seq[FfidmetadatamatchesRow]] = {
          ffidMetadataAndMatchesRows.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
        }
        ffidMetadataAndMatches.map {
          case (metadata, matches) => rowToFFIDMetadata(metadata, matches)
        }.toList
    }
  }

  private def generateFileStatusRows(ffidMetadata: FFIDMetadataInput): Future[List[FilestatusRow]] = {
    val fileId = ffidMetadata.fileId
    val timestamp = Timestamp.from(timeSource.now)

    for {
      consignments <- fileRepository.getConsignmentForFile(fileId)
      consignmentType = if (consignments.isEmpty) { throw InputDataException(s"No consignment found for file $fileId") }
      else { consignments.head.consignmenttype }
      statuses: List[Future[String]] = ffidMetadata.matches.map(m => checkStatus(m.puid, consignmentType))
      futureUniqueStatuses: Future[List[String]] = Future.sequence(statuses).map(statuses => statuses.distinct)
      uniqueStatuses <- futureUniqueStatuses
      rows = uniqueStatuses match {
        case s if uniqueStatuses.size == 1 =>
          List(FilestatusRow(uuidSource.uuid, fileId, FFID, s.head, timestamp))
        case _ => uniqueStatuses.filterNot(_.equals(Success)).map(
          FilestatusRow(uuidSource.uuid, fileId, FFID, _, timestamp))
      }
    } yield rows
  }

  def checkStatus(puidOption: Option[String], consignmentType: String): Future[String] = {
    val puid = puidOption.getOrElse("")
    for {
      disallowedPuidReason <- puidRepository.getDisallowedPuidReason(puid).map(_.getOrElse(""))
      allowedPuidExists <- puidRepository.checkAllowedPuidExists(puid)
    } yield {
      disallowedPuidReason match {
        case reason if reason == PasswordProtected => PasswordProtected
        case reason if reason == Zip => Zip
        case _ if consignmentType == ConsignmentType.judgment && !allowedPuidExists => NonJudgmentFormat
        case _ => Success
      }
    }
  }

  private def rowToFFIDMetadata(ffidMetadataRow: nationalarchives.Tables.FfidmetadataRow,
                                ffidMetadataMatchesRow: Seq[FfidmetadatamatchesRow]): FFIDMetadata = {
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
