package uk.gov.nationalarchives.tdr.api.service

import java.sql.{SQLException, Timestamp}
import java.util.UUID

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FFIDMetadataMatchesRepository, FFIDMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(ffidMetadataRepository: FFIDMetadataRepository,
                          matchesRepository: FFIDMetadataMatchesRepository,
                          fileRepository: FileRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FFIDMetadataService"))

  val passwordProtectedPuids: List[String] = List("fmt/494", "fmt/754", "fmt/755")
  val zipPuids: List[String] = List("fmt/289", "fmt/329", "fmt/484", "fmt/508", "fmt/600", "fmt/610", "fmt/613",
    "fmt/614", "fmt/625", "fmt/626", "fmt/639", "fmt/656", "fmt/726", "fmt/842", "fmt/843", "fmt/844", "fmt/850", "fmt/866",
    "fmt/887", "fmt/1070", "fmt/1071", "fmt/1087", "fmt/1095", "fmt/1096", "fmt/1097", "fmt/1098", "fmt/1100", "fmt/1102",
    "fmt/1105", "fmt/1190", "fmt/1242", "fmt/1243", "fmt/1244", "fmt/1245", "fmt/1251", "fmt/1252", "fmt/1281", "fmt/1340",
    "fmt/1341", "fmt/1355", "fmt/1361", "fmt/1399", "x-fmt/157", "x-fmt/219", "x-fmt/263", "x-fmt/265", "x-fmt/266", "x-fmt/267",
    "x-fmt/268", "x-fmt/269", "x-fmt/412", "x-fmt/416", "x-fmt/429")
  val judgmentPuidsAllow: List[String] = List("fmt/412")

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

  private def generateFileStatusRows(ffidMetadata: FFIDMetadataInput): Future[List[FilestatusRow]] = {
    val fileId = ffidMetadata.fileId
    val timestamp = Timestamp.from(timeSource.now)

    for {
      consignments <- fileRepository.getConsignmentForFile(fileId)
      consignmentType = if (consignments.isEmpty) { throw InputDataException(s"No consignment found for file $fileId") }
        else { consignments.head.consignmenttype }
      uniqueStatuses: List[String] = ffidMetadata.matches.map(m => checkStatus(m.puid, consignmentType)).distinct
      rows = uniqueStatuses match {
        case s if uniqueStatuses.size == 1 =>
          List(FilestatusRow(uuidSource.uuid, fileId, FFID, s.head, timestamp))
        case _ => uniqueStatuses.filterNot(_.equals(Success)).map(
          FilestatusRow(uuidSource.uuid, fileId, FFID, _, timestamp))
      }
    } yield rows
  }

  def checkStatus(puid: Option[String], consignmentType: String): String = {
    puid.getOrElse("") match {
      case p if passwordProtectedPuids.contains(p) => PasswordProtected
      case p if zipPuids.contains(p) => Zip
      case p if consignmentType == ConsignmentType.judgment && !judgmentPuidsAllow.contains(p) => NonJudgmentFormat
      case _ => Success
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
