package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
import java.sql.{SQLException, Timestamp}
import java.util.UUID

import uk.gov.nationalarchives.Tables.{FfidmetadataRow, FfidmetadatamatchesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FFIDMetadataMatchesRepository, FFIDMetadataRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.{FFIDMetadata, FFIDMetadataInput, FFIDMetadataMatches}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{FFID, PasswordProtected, Success, Zip}

import scala.concurrent.{ExecutionContext, Future}

class FFIDMetadataService(ffidMetadataRepository: FFIDMetadataRepository, matchesRepository: FFIDMetadataMatchesRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val executionContext: ExecutionContext) {

  val passwordProtectedPuids: List[String] = List("fmt/494", "fmt/754", "fmt/755")
  val zipPuids: List[String] = List("fmt/289", "fmt/329", "fmt/484", "fmt/508", "fmt/600", "fmt/610", "fmt/613",
    "fmt/614", "fmt/625", "fmt/626", "fmt/639", "fmt/656", "fmt/726", "fmt/842", "fmt/843", "fmt/844", "fmt/850", "fmt/866",
    "fmt/887", "fmt/1070", "fmt/1071", "fmt/1087", "fmt/1095", "fmt/1096", "fmt/1097", "fmt/1098", "fmt/1100", "fmt/1102",
    "fmt/1105", "fmt/1190", "fmt/1242", "fmt/1243", "fmt/1244", "fmt/1245", "fmt/1251", "fmt/1252", "fmt/1281", "fmt/1340",
    "fmt/1341", "fmt/1355", "fmt/1361", "fmt/1399", "x-fmt/157", "x-fmt/219", "x-fmt/263", "x-fmt/265", "x-fmt/266", "x-fmt/267",
    "x-fmt/268", "x-fmt/269", "x-fmt/412", "x-fmt/416", "x-fmt/429")

  def addFFIDMetadata(ffidMetadata: FFIDMetadataInput): Future[FFIDMetadata] = {
    val metadataRow = FfidmetadataRow(uuidSource.uuid, ffidMetadata.fileId,
      ffidMetadata.software,
      ffidMetadata.softwareVersion,
      Timestamp.from(timeSource.now),
      ffidMetadata.binarySignatureFileVersion,
      ffidMetadata.containerSignatureFileVersion,
      ffidMetadata.method)

    val fileStatusRows = generateFileStatusRows(ffidMetadata)

    def addFFIDMetadataMatches(ffidmetadataid: UUID): Future[Seq[Tables.FfidmetadatamatchesRow]] = {
      val matchRows = ffidMetadata.matches.map(m => FfidmetadatamatchesRow(ffidmetadataid, m.extension, m.identificationBasis, m.puid))
      matchesRepository.addFFIDMetadataMatches(matchRows)
    }

    (for {
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

  private def generateFileStatusRows(ffidMetadata: FFIDMetadataInput): List[FilestatusRow] = {
    val uniqueStatuses: List[String] = ffidMetadata.matches.map(m => checkStatus(m.puid)).distinct

    uniqueStatuses match {
      case s if uniqueStatuses.size == 1 =>
        List(FilestatusRow(uuidSource.uuid, ffidMetadata.fileId, FFID, s.head, Timestamp.from(timeSource.now)))
      case _ => uniqueStatuses.filterNot(_.equals(Success)).map(
        FilestatusRow(uuidSource.uuid, ffidMetadata.fileId, FFID, _, Timestamp.from(timeSource.now)))
    }
  }

  private def checkStatus(puid: Option[String]): String = {
    puid.getOrElse("") match {
      case p if passwordProtectedPuids.contains(p) => PasswordProtected
      case p if zipPuids.contains(p) => Zip
      case _ => Success
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
