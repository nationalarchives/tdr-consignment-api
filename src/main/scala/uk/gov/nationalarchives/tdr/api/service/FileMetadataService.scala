package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileMetadataUpdate, FileRepository}
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
    val distinctMetadataProperties: Set[UpdateFileMetadataInput] = input.metadataProperties.toSet
    val distinctPropertyNames: Set[String] = distinctMetadataProperties.map(_.filePropertyName)
    val uniqueFileIds: Seq[UUID] = input.fileIds.distinct

    for {
      existingFileRows <- fileRepository.getAllDescendants(uniqueFileIds)
      //Current policy is not to associate metadata with 'Folders'
      fileIds: Set[UUID] = existingFileRows.collect { case fileRow if fileRow.filetype.get == NodeType.fileTypeIdentifier => fileRow.fileid }.toSet
      existingFileMetadataRows: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(input.consignmentId, Some(fileIds), Some(distinctPropertyNames))
      fileIdToMetadataRows: Map[UUID, Seq[FilemetadataRow]] = existingFileMetadataRows.groupBy(_.fileid)

      propertyActions: Set[PropertyAction] = fileIds.flatMap {
        fileId => generatePropertyActions(fileId, distinctMetadataProperties, fileIdToMetadataRows)
      }
      groupedPropertyActions: Map[String, Set[PropertyAction]] = propertyActions.groupBy(_.updateActionType)
      addPropertyActions: Set[PropertyAction] = groupedPropertyActions.getOrElse("add", Set())
      updatePropertyActions: Set[PropertyAction] = groupedPropertyActions.getOrElse("update", Set())

      propertyUpdates: PropertyUpdates = generatePropertyUpdates(userId, addPropertyActions, updatePropertyActions)
      _ <- updateFileMetadata(propertyUpdates)
      fileIdsAdded: Set[UUID] = addPropertyActions.map(_.fileId)
      fileIdsUpdated: Set[UUID] = updatePropertyActions.map(_.fileId)
      metadataProperties = input.metadataProperties.map(metadataProperty => FileMetadata(metadataProperty.filePropertyName, metadataProperty.value))
    } yield BulkFileMetadata((fileIdsAdded ++ fileIdsUpdated).toSeq, metadataProperties)
  }

  private def generatePropertyActions(fileId: UUID, metadataProperties: Set[UpdateFileMetadataInput],
                                                existingFileMetadataRows: Map[UUID, Seq[FilemetadataRow]]): Set[PropertyAction] = {

    val existingPropertiesForFile: Map[String, Seq[FilemetadataRow]] = existingFileMetadataRows.getOrElse(fileId, Seq()).groupBy(_.propertyname)

    metadataProperties.map {
      metadataProperty =>
        if (!existingPropertiesForFile.contains(metadataProperty.filePropertyName)) {
          PropertyAction("add", metadataProperty.filePropertyName, metadataProperty.value, fileId, uuidSource.uuid)
        } else {
          val existingProperty: FilemetadataRow = existingPropertiesForFile(metadataProperty.filePropertyName).head
          val action = if (metadataProperty.value != existingProperty.value) {"update"} else {"noUpdate"}
          PropertyAction(action, metadataProperty.filePropertyName, metadataProperty.value, fileId, existingProperty.metadataid)
        }
    }
  }

  private def generatePropertyUpdates(userId: UUID, addPropertyActions: Set[PropertyAction], updatePropertyActions: Set[PropertyAction]): PropertyUpdates = {
    val propertiesRowsToAdd: Seq[FilemetadataRow] = addPropertyActions.map(
      addActionType => FilemetadataRow(
        addActionType.metadataId, addActionType.fileId, addActionType.propertyValue,
        Timestamp.from(timeSource.now), userId, addActionType.propertyName
      )
    ).toSeq

    val nameValueToUpdateActions: Map[(String, String), Set[PropertyAction]] = updatePropertyActions.groupBy(
      propertyUpdateRow => (propertyUpdateRow.propertyName, propertyUpdateRow.propertyValue)
    )

    val propertiesRowsToUpdate: Map[String, FileMetadataUpdate] = nameValueToUpdateActions.map {
      case ((propertyName, propertyValue), propertyUpdateActionType) =>
        val metadataIdsToUpdate: Set[UUID] = propertyUpdateActionType.map(_.metadataId)
        propertyName -> FileMetadataUpdate(metadataIdsToUpdate.toSeq, propertyName, propertyValue, Timestamp.from(timeSource.now), userId)
    }

    PropertyUpdates(propertiesRowsToAdd, propertiesRowsToUpdate)
  }

  private def updateFileMetadata(propertyUpdates: PropertyUpdates): Future[Unit] = {
    val propertiesRowsToAdd: Seq[FilemetadataRow] = propertyUpdates.rowsToAdd
    val propertiesRowsToUpdate: Map[String, FileMetadataUpdate] = propertyUpdates.rowsToUpdate
    val addFileMetadata: Future[Seq[FilemetadataRow]] = fileMetadataRepository.addFileMetadata(propertiesRowsToAdd)
    val updateFileMetadataProperties: Future[Seq[Int]] = fileMetadataRepository.updateFileMetadataProperties(propertiesRowsToUpdate)

    for {
      _ <- addFileMetadata// this is sequential but what about parallelism?
      updatedRows <- updateFileMetadataProperties
    } yield {
      val totalRowsUpdated: Int = updatedRows.sum
      val rowsToBeUpdated: Int = propertiesRowsToUpdate.values.map(_.metadataIds.size).sum

      if (totalRowsUpdated != rowsToBeUpdated) {
        throw new Exception(s"There was a problem: only $totalRowsUpdated out of $rowsToBeUpdated rows were updated!")
      }
    }
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
                                foiExemptionCode: Option[String])

  case class PropertyAction(updateActionType: String,
                            propertyName: String,
                            propertyValue: String,
                            fileId: UUID,
                            metadataId: UUID)

  case class PropertyUpdates(rowsToAdd: Seq[FilemetadataRow] = Seq(), rowsToUpdate: Map[String, FileMetadataUpdate] = Map())
}
