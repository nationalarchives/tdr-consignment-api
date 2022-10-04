package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataDelete, FileMetadataRepository, FileMetadataUpdate, FileRepository}
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
                          customMetadataPropertiesRepository: CustomMetadataPropertiesRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val ec: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def getCustomMetadataValuesWithDefault: Future[Seq[FilepropertyvaluesRow]] = customMetadataPropertiesRepository.getCustomMetadataValuesWithDefault

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
          fileStatusRows = Seq(getFileStatusRowForChecksumMatch(cfm.headOption, fileMetadataRow), getFileStatusRowForServerChecksum(fileMetadataRow))
          row <- fileMetadataRepository.addChecksumMetadata(fileMetadataRow, fileStatusRows)
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
        fileId => getPropertyActions(fileId, distinctMetadataProperties, fileIdToMetadataRows)
      }
      groupedPropertyActions: Map[String, Set[PropertyAction]] = propertyActions.groupBy(_.updateActionType)
      deletePropertyActions: Set[PropertyAction] = groupedPropertyActions.getOrElse("delete", Set())
      addPropertyActions: Set[PropertyAction] = groupedPropertyActions.getOrElse("add", Set())
      updatePropertyActions: Set[PropertyAction] = groupedPropertyActions.getOrElse("update", Set())

      propertyUpdates: PropertyUpdates = generatePropertyUpdates(userId, deletePropertyActions, addPropertyActions, updatePropertyActions)
      _ <- updateFileMetadata(input.consignmentId, propertyUpdates)
      fileIdsAdded: Set[UUID] = addPropertyActions.map(_.fileId)
      fileIdsUpdated: Set[UUID] = updatePropertyActions.map(_.fileId)
      metadataProperties = input.metadataProperties.map(metadataProperty => FileMetadata(metadataProperty.filePropertyName, metadataProperty.value))
    } yield BulkFileMetadata((fileIdsAdded ++ fileIdsUpdated).toSeq, metadataProperties)
  }

  private def getPropertyActions(fileId: UUID, metadataProperties: Set[UpdateFileMetadataInput],
                                      existingFileMetadataRows: Map[UUID, Seq[FilemetadataRow]]): Set[PropertyAction] = {

    val existingPropertiesForFile: Map[String, Seq[FilemetadataRow]] = existingFileMetadataRows.getOrElse(fileId, Seq()).groupBy(_.propertyname)
    val existingPropertyNameAndItsValues: Map[String, Seq[String]] = existingPropertiesForFile.map {
      case (propertyName, fileMetadataRow) => (propertyName, fileMetadataRow.map(_.value))
    }

    val metadataPropertiesGroupedByName: Set[((String, Boolean), Set[UpdateFileMetadataInput])] = metadataProperties.groupBy {
      metadataProperty => (metadataProperty.filePropertyName, metadataProperty.filePropertyIsMultiValue)
    }.toSet

    metadataPropertiesGroupedByName.flatMap {
      case ((metadataPropertyName, propertyIsMultiValue), propertyUpdates) =>
        val fileMetadataRowsForProperty: Set[FilemetadataRow] = existingPropertiesForFile.getOrElse(metadataPropertyName, Set()).toSet
        val existingValuesBelongingToProperty: Option[Seq[String]] = existingPropertyNameAndItsValues.get(metadataPropertyName)

        generatePropertyActions(fileId, fileMetadataRowsForProperty, existingValuesBelongingToProperty, propertyUpdates, propertyIsMultiValue)
    }
  }

  private def generatePropertyActions(fileId: UUID, fileMetadataRowsForProperty: Set[FilemetadataRow], existingValuesBelongingToProperty: Option[Seq[String]],
                                      propertyUpdates: Set[UpdateFileMetadataInput], propertyIsMultiValue: Boolean): Set[PropertyAction] = {
    existingValuesBelongingToProperty match {
      case None => propertyUpdates.map(propertyUpdate => PropertyAction("add", propertyUpdate.filePropertyName, propertyUpdate.value, fileId, uuidSource.uuid))
      case Some(valuesCurrentlyAssignedToProperty) =>
        if (propertyIsMultiValue) {
          val allNewValuesAreTheSame: Boolean = propertyUpdates.forall(propertyUpdate => valuesCurrentlyAssignedToProperty.contains(propertyUpdate.value))
          val actionToPerform: String = if (allNewValuesAreTheSame) "noUpdate" else "delete"
          val deleteOrNoUpdatePropertyActions: Set[PropertyAction] = fileMetadataRowsForProperty.map {
            fileMetadataRow => PropertyAction(actionToPerform, fileMetadataRow.propertyname, fileMetadataRow.value, fileId, fileMetadataRow.metadataid)
          }
          val addPropertyActions: Set[_ <: PropertyAction] =
            if (actionToPerform == "delete") { // delete the current fileMetadataRows and then add the new ones
              generatePropertyActions(fileId, fileMetadataRowsForProperty, existingValuesBelongingToProperty = None, propertyUpdates, propertyIsMultiValue)
            } else {Set()}

          deleteOrNoUpdatePropertyActions ++ addPropertyActions
        } else {
          propertyUpdates.map {
            propertyUpdate =>
              if (valuesCurrentlyAssignedToProperty.contains(propertyUpdate.value)) {
                PropertyAction("noUpdate", propertyUpdate.filePropertyName, propertyUpdate.value, fileId, uuidSource.uuid)
              } else {
                val existingProperty: FilemetadataRow = fileMetadataRowsForProperty.head
                PropertyAction("update", propertyUpdate.filePropertyName, propertyUpdate.value, fileId, existingProperty.metadataid)
              }
          }
        }
    }
  }

  private def generatePropertyUpdates(userId: UUID, deletePropertyActions: Set[PropertyAction], addPropertyActions: Set[PropertyAction],
                                      updatePropertyActions: Set[PropertyAction]): PropertyUpdates = {

    val metadataIdsToDelete: FileMetadataDelete =
      FileMetadataDelete(deletePropertyActions.map(_.fileId), deletePropertyActions.map(_.propertyName))

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

    PropertyUpdates(metadataIdsToDelete, propertiesRowsToAdd, propertiesRowsToUpdate)
  }

  private def updateFileMetadata(consignmentId: UUID, propertyUpdates: PropertyUpdates): Future[Unit] = {
    val metadataToDelete: FileMetadataDelete = propertyUpdates.metadataToDelete
    val propertiesRowsToAdd: Seq[FilemetadataRow] = propertyUpdates.rowsToAdd
    val propertiesRowsToUpdate: Map[String, FileMetadataUpdate] = propertyUpdates.rowsToUpdate
    val deleteFileMetadata: Future[Int] = fileMetadataRepository.deleteFileMetadata(metadataToDelete.fileIds, metadataToDelete.propertyNamesToDelete)
    val addFileMetadata: Future[Seq[FilemetadataRow]] = fileMetadataRepository.addFileMetadata(propertiesRowsToAdd)
    val updateFileMetadataProperties: Future[Seq[Int]] = fileMetadataRepository.updateFileMetadataProperties(propertiesRowsToUpdate)

    for {
      deletedRows <- deleteFileMetadata
      _ <- addFileMetadata
      updatedRows <- updateFileMetadataProperties
    } yield {
      val totalRowsDeleted: Int = deletedRows
      val rowsToBeDeleted: Int = metadataToDelete.fileIds.size * metadataToDelete.propertyNamesToDelete.size
      val totalRowsUpdated: Int = updatedRows.sum
      val rowsToBeUpdated: Int = propertiesRowsToUpdate.values.map(_.metadataIds.size).sum

      if (totalRowsDeleted != rowsToBeDeleted) {
        throw new Exception(s"There was a problem: only $totalRowsDeleted out of $rowsToBeDeleted rows were deleted")
      }

      if (totalRowsUpdated != rowsToBeUpdated) {
        throw new Exception(s"There was a problem: only $totalRowsUpdated out of $rowsToBeUpdated rows were updated")
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

  private def getFileStatusRowForChecksumMatch(checksumFileMetadata: Option[FilemetadataRow], fileMetadataInput: FilemetadataRow): FilestatusRow = {
    val fileStatus = checksumFileMetadata match {
      case Some(cfm) if cfm.value == fileMetadataInput.value => Success
      case Some(_) => Mismatch
      case None => throw new IllegalStateException(s"Cannot find client side checksum for file ${fileMetadataInput.fileid}")
    }
    loggingUtils.logFileFormatStatus("checksumMatch", fileMetadataInput.fileid, fileStatus)
    FilestatusRow(uuidSource.uuid, fileMetadataInput.fileid, ChecksumMatch, fileStatus, fileMetadataInput.datetime)
  }

  private def getFileStatusRowForServerChecksum(fileMetadataInput: FilemetadataRow): FilestatusRow = {
    val fileStatus = fileMetadataInput.value match {
      case "" => Failed
      case _ => Success
    }
    loggingUtils.logFileFormatStatus("serverChecksum", fileMetadataInput.fileid, fileStatus)
    FilestatusRow(uuidSource.uuid, fileMetadataInput.fileid, ServerChecksum, fileStatus, fileMetadataInput.datetime)
  }
}

object FileMetadataService {

  val SHA256ClientSideChecksum = "SHA256ClientSideChecksum"
  val ClientSideOriginalFilepath = "ClientSideOriginalFilepath"
  val ClientSideFileLastModifiedDate = "ClientSideFileLastModifiedDate"
  val ClientSideFileSize = "ClientSideFileSize"
  val ClosurePeriod = "ClosurePeriod"
  val ClosureStartDate = "ClosureStartDate"
  val FoiExemptionAsserted = "FoiExemptionAsserted"
  val TitlePublic = "TitlePublic"
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
  val clientSideProperties: List[String] = List(SHA256ClientSideChecksum, ClientSideOriginalFilepath, ClientSideFileLastModifiedDate, ClientSideFileSize)

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
      propertyNameMap.get(FoiExemptionCode.name),
      propertyNameMap.get(ClosurePeriod).map(_.toInt),
      propertyNameMap.get(ClosureStartDate).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(FoiExemptionAsserted).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(TitlePublic).map(_.toBoolean)
    )
  }

  case class StaticMetadata(name: String, value: String)

  case class FileMetadataValue(name: String, value: String)

  case class File(fileId: UUID,
                  fileType: Option[String] = None,
                  fileName: Option[String] = None,
                  parentId: Option[UUID] = None,
                  metadata: FileMetadataValues,
                  fileStatus: Option[String] = None,
                  ffidMetadata: Option[FFIDMetadata],
                  antivirusMetadata: Option[AntivirusMetadata],
                  originalFilePath: Option[String] = None,
                  fileMetadata: List[FileMetadataValue] = Nil)

  case class FileMetadataValues(sha256ClientSideChecksum: Option[String],
                                clientSideOriginalFilePath: Option[String],
                                clientSideLastModifiedDate: Option[LocalDateTime],
                                clientSideFileSize: Option[Long],
                                rightsCopyright: Option[String],
                                legalStatus: Option[String],
                                heldBy: Option[String],
                                language: Option[String],
                                foiExemptionCode: Option[String],
                                closurePeriod: Option[Int],
                                closureStartDate: Option[LocalDateTime],
                                foiExemptionAsserted: Option[LocalDateTime],
                                titlePublic: Option[Boolean])

  case class PropertyAction(updateActionType: String,
                            propertyName: String,
                            propertyValue: String,
                            fileId: UUID,
                            metadataId: UUID)

  case class PropertyUpdates(metadataToDelete: FileMetadataDelete,
                             rowsToAdd: Seq[FilemetadataRow] = Seq(),
                             rowsToUpdate: Map[String, FileMetadataUpdate] = Map())
}
