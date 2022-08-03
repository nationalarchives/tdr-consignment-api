package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
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

  implicit class PropertyUpdateActionGroupHelper(group: PropertyUpdateActionGroup) {
    def toMetadataRows(userId: UUID, metadataIdentifiers: Seq[MetadataIdentifiers]): Seq[FilemetadataRow] = {
      val timestamp = Timestamp.from(timeSource.now)
      metadataIdentifiers.map(i => FilemetadataRow(i.metadataId, i.fileId, group.propertyValue, timestamp, userId, group.propertyName))
    }
  }

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

  def updateBulkFileMetadata(input: UpdateBulkFileMetadataInput, userId: UUID): Future[BulkFileMetadata] = {
    val fileMetadataProperties: Seq[AddFileMetadataInput] = input.metadataProperties.distinct
    val propertyNames: Set[String] = fileMetadataProperties.map(_.filePropertyName).toSet
    val consignmentId = input.consignmentId

    val uniqueFileIds: Seq[UUID] = input.fileIds.distinct

    for {
      existingFiles <- fileRepository.getAllDescendants(uniqueFileIds)
      fileIds: Set[UUID] = existingFiles.filter(_.filetype.get == NodeType.fileTypeIdentifier).map(_.fileid).toSet
      existingFileMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(propertyNames))

      filesWithExistingProperty: Map[UUID, Seq[FilemetadataRow]] = existingFileMetadataProperties.groupBy(_.fileid)
      filesWithNoExistingProperties: Set[UUID] = fileIds.filterNot(fileId => filesWithExistingProperty.contains(fileId))

      metadataIdsSplitIntoAddUpdateOrNeither: Seq[FilePropertyUpdates] =
        groupFilesByAction(filesWithExistingProperty, filesWithNoExistingProperties, fileMetadataProperties)

      metadataIdsSplitIntoAddOrUpdateNonEmpty: Seq[FilePropertyUpdates] =
        metadataIdsSplitIntoAddUpdateOrNeither.filterNot {
          case update => update.propertyUpdateAction.updateAction == "doNothing" || update.metadataIdentifiers.isEmpty
        }

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

  private def groupFilesByAction(filesWithExistingMetadata: Map[UUID, Seq[FilemetadataRow]],
                                 filesWithNoMetadata: Set[UUID],
                                 metadataPropertiesInput: Seq[AddFileMetadataInput]): Seq[FilePropertyUpdates] = {
    val initialActionGrouping: Seq[FilePropertyUpdates] = {
      metadataPropertiesInput.map(
        fmp => FilePropertyUpdates(PropertyUpdateActionGroup("add", fmp.filePropertyName, fmp.value),
          filesWithNoMetadata.map(id => MetadataIdentifiers(uuidSource.uuid, id)))
      ) ++
        metadataPropertiesInput.map(
          fmp => FilePropertyUpdates(PropertyUpdateActionGroup("update", fmp.filePropertyName, fmp.value), Set())
        ) ++
        metadataPropertiesInput.map(
          fmp => FilePropertyUpdates(PropertyUpdateActionGroup("doNothing", fmp.filePropertyName, fmp.value), Set())
        )
    }

    filesWithExistingMetadata.foldLeft(initialActionGrouping: Seq[FilePropertyUpdates]) {
      case (metadataIdsSplitIntoAddUpdateOrNeither, (fileId, metadataRowsInDb)) =>


        val propertyNamesAlreadyAddedToFile: Map[String, (String, UUID)] =
          metadataRowsInDb.map(metadataRowInDb => (metadataRowInDb.propertyname, (metadataRowInDb.value, metadataRowInDb.metadataid))).toMap

        val updatedMetadataIdsSplitIntoAddUpdateOrNeither: Seq[FilePropertyUpdates] =
          metadataPropertiesInput.map {
            case input =>
              val propertyName = input.filePropertyName
              val value = input.value
              val (actionName, metadataId) =
                if (propertyNamesAlreadyAddedToFile.contains(propertyName)) {
                  val (valueCurrentlyOnProperty: String, metadataId: UUID) = propertyNamesAlreadyAddedToFile(propertyName)
                  if (valueCurrentlyOnProperty == value) {
                    (PropertyUpdateActionGroup("doNothing", propertyName, value), metadataId)
                  } else {
                    (PropertyUpdateActionGroup("update", propertyName, value), metadataId)
                  }
                } else {
                  (PropertyUpdateActionGroup("add", propertyName, value), uuidSource.uuid)
                }

              //val a: String = actionName
              val metadataIdsToAddOrUpdatePropertyName: Seq[FilePropertyUpdates] = metadataIdsSplitIntoAddUpdateOrNeither.filter(
                _.propertyUpdateAction == actionName)
              //actionName -> (metadataIdsToAddOrUpdatePropertyName, MetadataIdentifiers(metadataId, fileId))
              metadataIdsToAddOrUpdatePropertyName
          }
        metadataIdsSplitIntoAddUpdateOrNeither ++ updatedMetadataIdsSplitIntoAddUpdateOrNeither
    }
  }

  private def addOrUpdateFileMetadata(metadataIdsSplitIntoAddOrUpdate: Seq[FilePropertyUpdates],
                                      timestamp: Timestamp, userId: UUID): Iterable[Future[Seq[FilemetadataRow]]] = {

    def convertMetadataIdsToFileMetadataRows(filePropertyName: String, value: String, metadataIdsAndFileIds: Seq[MetadataIdentifiers]): Seq[FilemetadataRow] =
      metadataIdsAndFileIds.map { case identifiers => FilemetadataRow(identifiers.metadataId, identifiers.fileId, value, timestamp, userId, filePropertyName) }

    val metadataIdGroupsSplitIntoAddOrUpdate: Map[String, Seq[FilePropertyUpdates]] =
      metadataIdsSplitIntoAddOrUpdate.groupBy { case update => update.propertyUpdateAction.updateAction }

    val addedFileMetadataRows: Future[Seq[FilemetadataRow]] = metadataIdGroupsSplitIntoAddOrUpdate.get("add") match {
      case Some(addMetadataIdGroups) =>
        val addFileMetadataRows = addMetadataIdGroups.flatMap {
          case update => convertMetadataIdsToFileMetadataRows(
            update.propertyUpdateAction.propertyName, update.propertyUpdateAction.propertyValue, update.metadataIdentifiers.toSeq)
        }
        fileMetadataRepository.addFileMetadata(addFileMetadataRows)
      case _ => Future(Nil)
    }

    val updatedFileMetadataRows: Iterable[Future[Seq[FilemetadataRow]]] = metadataIdGroupsSplitIntoAddOrUpdate.get("update") match {
      case Some(updateMetadataIdGroups) =>
        updateMetadataIdGroups.map {
          case update =>
            val fileMetadataRows: Seq[FilemetadataRow] = convertMetadataIdsToFileMetadataRows(
              update.propertyUpdateAction.propertyName, update.propertyUpdateAction.propertyValue, update.metadataIdentifiers.toSeq)
            val metadataIds: Seq[UUID] = update.metadataIdentifiers.map(_.metadataId).toSeq
            val metadataIdsLength = metadataIds.length
            for {
              updateStatus <- fileMetadataRepository.updateFileMetadata(
                metadataIds, update.propertyUpdateAction.propertyName, update.propertyUpdateAction.propertyValue, timestamp, userId)
            } yield updateStatus match {
              case `metadataIdsLength` => fileMetadataRows
              case _ => throw new Exception(s"There was a problem when trying to update the value of the ${update.propertyUpdateAction.propertyName} property.")
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

  case class MetadataIdentifiers(metadataId: UUID, fileId: UUID)
  case class PropertyUpdateActionGroup(updateAction: String, propertyName: String, propertyValue: String)
  case class FilePropertyUpdates(propertyUpdateAction: PropertyUpdateActionGroup, metadataIdentifiers: Set[MetadataIdentifiers])
}
