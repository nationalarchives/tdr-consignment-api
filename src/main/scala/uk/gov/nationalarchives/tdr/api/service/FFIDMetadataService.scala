package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataInputValues, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(
    ffidMetadataRepository: FFIDMetadataRepository,
    matchesRepository: FFIDMetadataMatchesRepository,
    fileRepository: FileRepository,
    allowedPuidsRepository: AllowedPuidsRepository,
    disallowedPuidsRepository: DisallowedPuidsRepository,
    timeSource: TimeSource,
    uuidSource: UUIDSource
)(implicit val executionContext: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FFIDMetadataService"))

  @deprecated("Use addFFIDMetadata(ffidMetadataInput: FFIDMetadataInput)")
  def addFFIDMetadata(ffidMetadataInputValues: FFIDMetadataInputValues): Future[FFIDMetadata] = {
    addFFIDMetadata(FFIDMetadataInput(ffidMetadataInputValues :: Nil)).map(_.head)
  }

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

    for {
      statuses <- Future.sequence(ffidMetadataInput.metadataInputValues.map(generateFileStatusRows))
      ffidMetadata <- ffidMetadataRepository.addFFIDMetadata(metadataRows, statuses.flatten)
      matches <- matchesRepository.addFFIDMetadataMatches(matchRows.flatten)
    } yield {
      statuses.flatten.groupBy(_.fileid).foreach {
        case (fileId, statuses) => loggingUtils.logFileFormatStatus("FFID", fileId, statuses.map(_.value).mkString(","))
      }
      val matchesMap = matches.groupBy(_.ffidmetadataid)
      ffidMetadata.map(ffid => rowToFFIDMetadata(ffid, matchesMap(ffid.ffidmetadataid)))
    }
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

  private def generateFileStatusRows(ffidMetadata: FFIDMetadataInputValues): Future[List[FilestatusRow]] = {
    val fileId = ffidMetadata.fileId
    val timestamp = Timestamp.from(timeSource.now)

    for {
      consignments <- fileRepository.getConsignmentForFile(fileId)
      consignmentType =
        if (consignments.isEmpty) { throw InputDataException(s"No consignment found for file $fileId") }
        else { consignments.head.consignmenttype }
      statuses: List[Future[String]] = ffidMetadata.matches.map(m => checkStatus(m.puid.getOrElse(""), consignmentType))
      ffidStatuses: List[String] <- Future.sequence(statuses).map(statuses => statuses.distinct)
      rows = ffidStatuses match {
        case s if ffidStatuses.size == 1 =>
          List(FilestatusRow(uuidSource.uuid, fileId, FFID, s.head, timestamp))
        case _ => ffidStatuses.filterNot(_.equals(Success)).map(FilestatusRow(uuidSource.uuid, fileId, FFID, _, timestamp))
      }
    } yield rows
  }

  def checkStatus(puid: String, consignmentType: String): Future[String] = {
    if (consignmentType == ConsignmentType.judgment) {
      checkJudgmentStatus(puid)
    } else {
      checkStandardStatus(puid)
    }
  }

  def checkJudgmentStatus(puid: String): Future[String] = {
    // Need to check if the disallowed puid is active to prevent the following scenario:
    // Upload of a file which is an inactive disallowed puid type, this is not a judgment format, but is inactive
    // this means it would pass through the all checks succeeded stage and be transferred when it shouldn't
    for {
      allowedPuid <- allowedPuidsRepository.checkAllowedPuidExists(puid)
      disallowedPuidRow <- disallowedPuidsRepository.getDisallowedPuid(puid)
    } yield disallowedPuidRow match {
      case Some(value) if value.active => value.reason
      case _ if !allowedPuid           => NonJudgmentFormat
      case _                           => Success
    }
  }

  def checkStandardStatus(puid: String): Future[String] = {
    for {
      disallowedPuidRow <- disallowedPuidsRepository.getDisallowedPuid(puid)
    } yield disallowedPuidRow match {
      case Some(value) => value.reason
      case _           => Success
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
