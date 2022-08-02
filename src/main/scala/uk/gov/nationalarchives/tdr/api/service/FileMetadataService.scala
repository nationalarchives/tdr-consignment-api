package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataService(fileMetadataRepository: FileMetadataRepository,
                          fileRepository: FileRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val ec: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def addStaticMetadata(files: Seq[FileRow], userId: UUID): Future[Seq[FilemetadataRow]] = {
    val now = Timestamp.from(timeSource.now)
    val fileMetadataRows = for {
      staticMetadata <- staticMetadataProperties
      fileId <- files.map(_.fileid)
    } yield FilemetadataRow(uuidSource.uuid, fileId, staticMetadata.value, now, userId, staticMetadata.name)
    fileMetadataRepository.addFileMetadata(fileMetadataRows)
  }

  def addFileMetadata(addFileMetadataInput: AddFileMetadataWithFileIdInput, userId: UUID): Future[FileMetadataWithFileId] = {
    val fileMetadataRow =
      FilemetadataRow(uuidSource.uuid, addFileMetadataInput.fileId,
        addFileMetadataInput.value,
        Timestamp.from(timeSource.now),
        userId, addFileMetadataInput.filePropertyName)

    fileMetadataRow.propertyname match {
      case SHA256ServerSideChecksum => {
        for {
          cfm <- fileMetadataRepository.getFileMetadataByProperty(fileMetadataRow.fileid, SHA256ClientSideChecksum)
          fileStatus: String = cfm.headOption match {
            case Some(cfm) if cfm.value == fileMetadataRow.value => Success
            case Some(_) => Mismatch
            case None => throw new IllegalStateException(s"Cannot find client side checksum for file ${fileMetadataRow.fileid}")
          }
          fileStatusRow: FilestatusRow = FilestatusRow(uuidSource.uuid, fileMetadataRow.fileid, ChecksumMatch, fileStatus, fileMetadataRow.datetime)
          _ <- Future(loggingUtils.logFileFormatStatus("checksumMatch", fileMetadataRow.fileid, fileStatus))
          row <- fileMetadataRepository.addChecksumMetadata(fileMetadataRow, fileStatusRow)
        } yield FileMetadataWithFileId(fileMetadataRow.propertyname, row.fileid, row.value)
      } recover {
        case e: Throwable =>
          throw InputDataException(s"Could not find metadata for file ${fileMetadataRow.fileid}", Some(e))
      }
      case _ => Future.failed(InputDataException(s"${fileMetadataRow.propertyname} found. We are only expecting checksum updates for now"))
    }
  }

  def addElseUpdateBulkFileMetadata(addElseUpdateBulkFileMetadataInput: AddElseUpdateBulkFileMetadataInput, userId: UUID): Future[BulkFileMetadata] = {
    val fileMetadataProperties: Seq[AddFileMetadataInput] = addElseUpdateBulkFileMetadataInput.metadataProperties.distinct
    val filePropertyNameAndItsValue: Map[String, String] = fileMetadataProperties.map {
      fileMetadataProperty => (fileMetadataProperty.filePropertyName, fileMetadataProperty.value)
    }.toMap

    val uniqueFileIds: Seq[UUID] = addElseUpdateBulkFileMetadataInput.fileIds.distinct

    for {
      fileRows <- fileRepository.getAllDescendants(uniqueFileIds)
      fileIds: Set[UUID] = fileRows.collect { case fileRow if fileRow.filetype.get == "File" => fileRow.fileid }.toSet
      fileMetadataRowsWithPertinentPropertyNames: Seq[FilemetadataRow] <-
        fileMetadataRepository.getFileMetadata(addElseUpdateBulkFileMetadataInput.consignmentId, Some(fileIds), Some(filePropertyNameAndItsValue.keys.toSet))

      idsOfFilesWithAtLeastOnePertinentProperty: Map[UUID, Seq[FilemetadataRow]] = fileMetadataRowsWithPertinentPropertyNames.groupBy(_.fileid)
      idsOfFilesWithNoRelevantMetadataProperties: Set[UUID] = fileIds.filterNot(fileId => idsOfFilesWithAtLeastOnePertinentProperty.contains(fileId))

      metadataIdsSplitIntoAddUpdateOrNeither: Map[(String, String, String), Seq[(UUID, UUID)]] =
        splitFileIdsIntoAddUpdateOrNeitherGroups(idsOfFilesWithAtLeastOnePertinentProperty, idsOfFilesWithNoRelevantMetadataProperties, fileMetadataProperties)

      metadataIdsSplitIntoAddOrUpdateNonEmpty: Map[(String, String, String), Seq[(UUID, UUID)]] =
        metadataIdsSplitIntoAddUpdateOrNeither.filterNot { case ((action, _, _), ids) => action == "doNothing" || ids.isEmpty }

      timestamp = Timestamp.from(timeSource.now)
      metadataRows <- {
        val resultsPerPropertyName: Iterable[Future[Seq[FilemetadataRow]]] = addOrUpdateFileMetadata(metadataIdsSplitIntoAddOrUpdateNonEmpty, timestamp, userId)
        val futureOfResultsPerPropertyName: Future[Iterable[Seq[FilemetadataRow]]] = Future.sequence(resultsPerPropertyName)
        val allFileMetadataResultsInOne: Future[Iterable[FilemetadataRow]] =
          for (resultsPerPropertyName <- futureOfResultsPerPropertyName) yield resultsPerPropertyName.flatten
        allFileMetadataResultsInOne
      }

      fileIdsAndMetadata = metadataRows.foldLeft((Set[UUID](), Set[FileMetadata]())) {
        (fileIdsAndMetadata, fileMetadataRow) =>
          (
            fileIdsAndMetadata._1 + fileMetadataRow.fileid,
            fileIdsAndMetadata._2 + FileMetadata(fileMetadataRow.propertyname, fileMetadataRow.value)
          )
      }
    } yield BulkFileMetadata(fileIdsAndMetadata._1.toSeq, fileIdsAndMetadata._2.toSeq)
  }

  private def splitFileIdsIntoAddUpdateOrNeitherGroups(idsOfFilesWithMetadata: Map[UUID, Seq[FilemetadataRow]], idsOfFilesWithNoMetadata: Set[UUID],
                                                       fileMetadataPropertiesToAddOrUpdate: Seq[AddFileMetadataInput]) = {
    val initialMetadataIdsSplitIntoAddUpdateOrNeither: Map[(String, String, String), Seq[(UUID, UUID)]] = {
      fileMetadataPropertiesToAddOrUpdate.map(
        fmp => ("add", fmp.filePropertyName, fmp.value) ->
          idsOfFilesWithNoMetadata.map(idOfFileWithNoMetadata => (uuidSource.uuid, idOfFileWithNoMetadata)).toSeq
      ) ++
        fileMetadataPropertiesToAddOrUpdate.map(
          fmp => ("update", fmp.filePropertyName, fmp.value) -> Seq()
        ) ++
        fileMetadataPropertiesToAddOrUpdate.map(
          fmp => ("doNothing", fmp.filePropertyName, fmp.value) -> Seq()
        )
    }.toMap

    val propertyNamesAndValuesToAddOrUpdate: Seq[(String, String)] = fileMetadataPropertiesToAddOrUpdate.map(fmp => (fmp.filePropertyName, fmp.value))

    idsOfFilesWithMetadata.foldLeft(initialMetadataIdsSplitIntoAddUpdateOrNeither: Map[(String, String, String), Seq[(UUID, UUID)]]) {
      case (metadataIdsSplitIntoAddUpdateOrNeither, (fileId, metadataRowsInDb)) =>
        val propertyNamesAlreadyAddedToFile: Map[String, (String, UUID)] =
          metadataRowsInDb.map(metadataRowInDb => (metadataRowInDb.propertyname, (metadataRowInDb.value, metadataRowInDb.metadataid))).toMap

        val updatedMetadataIdsSplitIntoAddUpdateOrNeither: Map[(String, String, String), Seq[(UUID, UUID)]] =
          propertyNamesAndValuesToAddOrUpdate.map {
            case (propertyName, valueToAddOrUpdate) =>
              val (actionName, metadataId) =
                if (propertyNamesAlreadyAddedToFile.contains(propertyName)) {
                  val (valueCurrentlyOnProperty: String, metadataId: UUID) = propertyNamesAlreadyAddedToFile(propertyName)
                  if (valueCurrentlyOnProperty == valueToAddOrUpdate) {
                    (("doNothing", propertyName, valueToAddOrUpdate), metadataId)
                  } else {
                    (("update", propertyName, valueToAddOrUpdate), metadataId)
                  }
                } else {
                  (("add", propertyName, valueToAddOrUpdate), uuidSource.uuid)
                }

              val metadataIdsToAddOrUpdatePropertyName: Seq[(UUID, UUID)] = metadataIdsSplitIntoAddUpdateOrNeither(actionName)
              actionName -> metadataIdsToAddOrUpdatePropertyName.+:(metadataId, fileId)
          }.toMap
        metadataIdsSplitIntoAddUpdateOrNeither ++ updatedMetadataIdsSplitIntoAddUpdateOrNeither
    }
  }

  private def addOrUpdateFileMetadata(metadataIdsSplitIntoAddOrUpdate: Map[(String, String, String), Seq[(UUID, UUID)]],
                                      timestamp: Timestamp, userId: UUID): Iterable[Future[Seq[FilemetadataRow]]] = {

    def convertMetadataIdsToFileMetadataRows(filePropertyName: String, value: String, metadataIdsAndFileIds: Seq[(UUID, UUID)]) =
      metadataIdsAndFileIds.map { case (metadataId, fileId) => FilemetadataRow(metadataId, fileId, value, timestamp, userId, filePropertyName) }

    val metadataIdGroupsSplitIntoAddOrUpdate: Map[String, Map[(String, String, String), Seq[(UUID, UUID)]]] =
      metadataIdsSplitIntoAddOrUpdate.groupBy { case ((addOrUpdateAction, _, _), _) => addOrUpdateAction }

    val addedFileMetadataRows: Future[Seq[FilemetadataRow]] = metadataIdGroupsSplitIntoAddOrUpdate.get("add") match {
      case Some(addMetadataIdGroups) =>
        val addFileMetadataRows = addMetadataIdGroups.flatMap {
          case ((_, filePropertyName, value), metadataIdsAndFileIds) => convertMetadataIdsToFileMetadataRows(filePropertyName, value, metadataIdsAndFileIds)
        }.toSeq
        fileMetadataRepository.addFileMetadata(addFileMetadataRows)
      case _ => Future(Nil)
    }

    val updatedFileMetadataRows: Iterable[Future[Seq[FilemetadataRow]]] = metadataIdGroupsSplitIntoAddOrUpdate.get("update") match {
      case Some(updateMetadataIdGroups) =>
        updateMetadataIdGroups.map {
          case ((_, filePropertyName, value), metadataIdsAndFileIds) =>
            val fileMetadataRows: Seq[FilemetadataRow] = convertMetadataIdsToFileMetadataRows(filePropertyName, value, metadataIdsAndFileIds)
            val metadataIds: Seq[UUID] = metadataIdsAndFileIds.map(metadataIdsAndFileIds => metadataIdsAndFileIds._1)
            val metadataIdsLength = metadataIds.length
            for {
              updateStatus <- fileMetadataRepository.updateFileMetadata(metadataIds, filePropertyName, value, timestamp, userId)
            } yield updateStatus match {
              case `metadataIdsLength` => fileMetadataRows
              case _ => throw new Exception(s"There was a problem when trying to update the value of the $filePropertyName property.")
            }
        }
      case _ => Seq(Future(Nil))
    }

    updatedFileMetadataRows ++: Seq(addedFileMetadataRows)
  }

  def getFileMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, FileMetadataValues]] =
    fileMetadataRepository.getFileMetadata(consignmentId, selectedFileIds).map {
      rows =>
        rows.groupBy(_.fileid).map {
          case (fileId, fileMetadata) => fileId -> getFileMetadataValues(fileMetadata)
        }
    }
}

object FileMetadataService {

  val SHA256ClientSideChecksum = "SHA256ClientSideChecksum"
  val ClientSideOriginalFilepath = "ClientSideOriginalFilepath"
  val ClientSideFileLastModifiedDate = "ClientSideFileLastModifiedDate"
  val ClientSideFileSize = "ClientSideFileSize"
  /**
   * Save default values for these properties because TDR currently only supports records which are Open, in English, etc.
   * Users agree to these conditions at a consignment level, so it's OK to save these as defaults for every file.
   * They need to be saved so they can be included in the export package.
   * The defaults may be removed in future once we let users upload a wider variety of records.
   */
  val RightsCopyright: StaticMetadata = StaticMetadata("RightsCopyright", "Crown Copyright")
  val LegalStatus: StaticMetadata = StaticMetadata("LegalStatus", "Public Record")
  val HeldBy: StaticMetadata = StaticMetadata("HeldBy", "TNA")
  val Language: StaticMetadata = StaticMetadata("Language", "English")
  val FoiExemptionCode: StaticMetadata = StaticMetadata("FoiExemptionCode", "open")
  val clientSideProperties = List(SHA256ClientSideChecksum, ClientSideOriginalFilepath, ClientSideFileLastModifiedDate, ClientSideFileSize)
  val staticMetadataProperties = List(RightsCopyright, LegalStatus, HeldBy, Language, FoiExemptionCode)

  def getFileMetadataValues(fileMetadataRow: Seq[FilemetadataRow]): FileMetadataValues = {
    val propertyNameMap: Map[String, String] = fileMetadataRow.groupBy(_.propertyname).transform {
      (_, value) => value.head.value
    }
    FileMetadataValues(
      propertyNameMap.get(SHA256ClientSideChecksum),
      propertyNameMap.get(ClientSideOriginalFilepath),
      propertyNameMap.get(ClientSideFileLastModifiedDate).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(ClientSideFileSize).map(_.toLong),
      propertyNameMap.get(RightsCopyright.name),
      propertyNameMap.get(LegalStatus.name),
      propertyNameMap.get(HeldBy.name),
      propertyNameMap.get(Language.name),
      propertyNameMap.get(FoiExemptionCode.name)
    )
  }

  case class StaticMetadata(name: String, value: String)

  case class File(fileId: UUID,
                  fileType: Option[String] = None,
                  fileName: Option[String] = None,
                  parentId: Option[UUID] = None,
                  metadata: FileMetadataValues,
                  fileStatus: Option[String] = None,
                  ffidMetadata: Option[FFIDMetadata],
                  antivirusMetadata: Option[AntivirusMetadata])

  case class FileMetadataValues(sha256ClientSideChecksum: Option[String],
                                clientSideOriginalFilePath: Option[String],
                                clientSideLastModifiedDate: Option[LocalDateTime],
                                clientSideFileSize: Option[Long],
                                rightsCopyright: Option[String],
                                legalStatus: Option[String],
                                heldBy: Option[String],
                                language: Option[String],
                                foiExemptionCode: Option[String]
                               )
}
