package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{NotEntered, _}
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataService(
    fileMetadataRepository: FileMetadataRepository,
    fileRepository: FileRepository,
    customMetadataPropertiesRepository: CustomMetadataPropertiesRepository,
    fileStatusRepository: FileStatusRepository,
    customMetadataService: CustomMetadataPropertiesService,
    timeSource: TimeSource,
    uuidSource: UUIDSource
)(implicit val ec: ExecutionContext) {

  implicit class FileRowsHelper(fileRows: Seq[Tables.FileRow]) {
    def toFileTypeIds: Set[UUID] = {
      fileRows.collect { case fileRow if fileRow.filetype.get == NodeType.fileTypeIdentifier => fileRow.fileid }.toSet
    }
  }

  implicit class CustomMetadataFieldsHelper(fields: Seq[CustomMetadataField]) {
    def toPropertyNames: Set[String] = fields.map(_.name).toSet

    def toGroupPropertyNames: Map[String, Set[String]] = {
      Map(ClosureMetadata -> closureFields.toPropertyNames, DescriptiveMetadata -> descriptiveFields.toPropertyNames)
    }

    def closureFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryClosure") || f.propertyGroup.contains("OptionalClosure"))
    }

    def descriptiveFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryMetadata") || f.propertyGroup.contains("OptionalMetadata"))
    }

    def valueToDependencies: Map[String, List[CustomMetadataField]] = {
      fields
        .flatMap(f => {
          val values: List[CustomMetadataValues] = f.values
          values.map(v => {
            v.value -> v.dependencies
          })
        })
        .toMap
    }
  }

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def getSumOfFileSizes(consignmentId: UUID): Future[Int] = fileMetadataRepository.getSumOfFileSizes(consignmentId)

  def getCustomMetadataValuesWithDefault: Future[Seq[FilepropertyvaluesRow]] = customMetadataPropertiesRepository.getCustomMetadataValuesWithDefault

  def addFileMetadata(addFileMetadataInput: AddFileMetadataWithFileIdInput, userId: UUID): Future[FileMetadataWithFileId] = {
    val fileMetadataRow =
      FilemetadataRow(uuidSource.uuid, addFileMetadataInput.fileId, addFileMetadataInput.value, Timestamp.from(timeSource.now), userId, addFileMetadataInput.filePropertyName)

    fileMetadataRow.propertyname match {
      case SHA256ServerSideChecksum =>
        {
          for {
            cfm <- fileMetadataRepository.getFileMetadataByProperty(fileMetadataRow.fileid, SHA256ClientSideChecksum)
            fileStatusRows = Seq(getFileStatusRowForChecksumMatch(cfm.headOption, fileMetadataRow), getFileStatusRowForServerChecksum(fileMetadataRow))
            row <- fileMetadataRepository.addChecksumMetadata(fileMetadataRow, fileStatusRows)
          } yield FileMetadataWithFileId(fileMetadataRow.propertyname, row.fileid, row.value)
        } recover { case e: Throwable =>
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
      fileIds: Set[UUID] = existingFileRows.toFileTypeIds
      _ <- fileMetadataRepository.deleteFileMetadata(fileIds, distinctPropertyNames)
      addedRows <- fileMetadataRepository.addFileMetadata(generateFileMetadataRows(fileIds, distinctMetadataProperties, userId))
      // call the validate metadata after the metadata has been added to DB
      _ <- updateCustomMetadataStatuses(uniqueFileIds.toSet, input.consignmentId, distinctPropertyNames)
      metadataPropertiesAdded = addedRows.map(r => { FileMetadata(r.propertyname, r.value) }).toSet
    } yield BulkFileMetadata(fileIds.toSeq, metadataPropertiesAdded.toSeq)
  }

  case class FilePropertyState(fileId: UUID, propertyName: String, propertyValue: Option[String], valid: Boolean)

  private def validateFileProperty(fileIds: Set[UUID], fieldToValidate: CustomMetadataField, existingProperties: Seq[FilemetadataRow]): Set[FilePropertyState] = {
    val propertyToValidateName: String = fieldToValidate.name
    val valueToDependencies: Map[String, List[CustomMetadataField]] = Seq(fieldToValidate).valueToDependencies

    fileIds.flatMap(id => {
      val allExistingFileProperties = existingProperties.filter(_.fileid == id)
      val existingFilePropertiesToValidate: Seq[FilemetadataRow] = allExistingFileProperties.filter(_.propertyname == propertyToValidateName)

      if (existingFilePropertiesToValidate.isEmpty) {
        Set(FilePropertyState(id, propertyToValidateName, None, valid = true))
      } else {
        existingFilePropertiesToValidate.map(existingProperty => {
          val valueDependencies: Option[List[CustomMetadataField]] = valueToDependencies.get(existingProperty.value)
          val valid: Boolean = valueDependencies match {
            case Some(dependencies) => dependencies.toPropertyNames.subsetOf(allExistingFileProperties.map(_.propertyname).toSet)
            case None               => true
          }
          FilePropertyState(id, propertyToValidateName, Some(existingProperty.value), valid)
        })
      }
    })
  }

  def updateCustomMetadataStatuses(fileIds: Set[UUID], consignmentId: UUID, updatedProperties: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(customMetadataFields.toPropertyNames))
    } yield {
      val propertyStates: List[FilePropertyState] = updatedProperties
        .flatMap(propertyName => {
          val propertyToValidate: Option[CustomMetadataField] = customMetadataFields.find(_.name == propertyName)
          propertyToValidate match {
            case Some(field) => validateFileProperty(fileIds, field, existingMetadataProperties)
            case _           => Set()
          }
        })
        .toList

      val allFileStatuses = customMetadataFields.toGroupPropertyNames
        .flatMap(group => {
          val groupName = group._1
          val groupProperties = group._2
          groupProperties.flatMap(groupProperty => {
            // how to handle multiple value properties, eg if allow for multiple dates for multiple FOI codes?
            val states: List[FilePropertyState] = propertyStates.filter(_.propertyName == groupProperty)
            states.map(state => {
              val status: String = if (state.propertyValue.isEmpty) NotEntered else if (state.valid) Completed else Incomplete
              FilestatusRow(UUID.randomUUID(), state.fileId, groupName, status, Timestamp.from(timeSource.now))
            })
          })
        })
        .toList

      if (allFileStatuses.nonEmpty) {
        val statusesToDelete: Set[String] = allFileStatuses.map(_.statustype).toSet
        fileStatusRepository.deleteFileStatus(fileIds, statusesToDelete)
        fileStatusRepository.addFileStatuses(allFileStatuses)
      }

      allFileStatuses
    }
  }

  def deleteFileMetadata(input: DeleteFileMetadataInput, userId: UUID): Future[DeleteFileMetadata] = {

    for {
      existingFileRows <- fileRepository.getAllDescendants(input.fileIds.distinct)
      fileIds: Set[UUID] = existingFileRows.toFileTypeIds
      metadataValues <- customMetadataPropertiesRepository.getCustomMetadataValues
      groupId = metadataValues
        .find(property => property.propertyname == ClosureType && property.propertyvalue == "Closed")
        .map(_.dependencies)
        .getOrElse(throw new IllegalStateException("Can't find metadata property 'ClosureType' with value 'Closed' in the db"))
      dependencies <- customMetadataPropertiesRepository.getCustomMetadataDependencies
      propertyNames = dependencies.filter(dependency => groupId.contains(dependency.groupid)).map(_.propertyname)

      (fileMetadataToUpdate, fileMetadataToDelete) = getFileMetadataToUpdateAndDelete(metadataValues, propertyNames, userId)
      _ <- fileMetadataRepository.updateFileMetadataProperties(fileIds, fileMetadataToUpdate)
      _ <- fileMetadataRepository.deleteFileMetadata(fileIds, fileMetadataToDelete.toSet)
      _ <- updateCustomMetadataStatuses(fileIds, existingFileRows.map(_.consignmentid).head, (fileMetadataToDelete ++ fileMetadataToUpdate.keys).toSet)
    } yield DeleteFileMetadata(fileIds.toSeq, propertyNames)
  }

  private def getFileMetadataToUpdateAndDelete(
      metadataValues: Seq[FilepropertyvaluesRow],
      propertyNames: Seq[String],
      userId: UUID
  ): (Map[String, FileMetadataUpdate], Seq[String]) = {
    val metadataValuesWithDefault = metadataValues.filter(_.default.contains(true))
    val (fileMetadataToUpdate, fileMetadataToDelete) = propertyNames.partition(name => metadataValuesWithDefault.exists(_.propertyname == name))

    val update: (String, String) => FileMetadataUpdate = FileMetadataUpdate(Nil, _, _, Timestamp.from(timeSource.now), userId)

    val fileMetadataUpdates =
      fileMetadataToUpdate.map(propertyName => propertyName -> update(propertyName, metadataValuesWithDefault.find(_.propertyname == propertyName).head.propertyvalue)).toMap

    val additionalFileMetadataUpdate = metadataValuesWithDefault
      .find(_.propertyname == ClosureType)
      .map(property => property.propertyname -> update(property.propertyname, property.propertyvalue))
      .head

    (fileMetadataUpdates + additionalFileMetadataUpdate, fileMetadataToDelete)
  }

  private def generateFileMetadataRows(fileIds: Set[UUID], inputs: Set[UpdateFileMetadataInput], userId: UUID): List[FilemetadataRow] = {
    fileIds
      .flatMap(id =>
        {
          inputs.map(i => FilemetadataRow(UUID.randomUUID(), id, i.value, Timestamp.from(timeSource.now), userId, i.filePropertyName))
        }.toList
      )
      .toList
  }

  def getFileMetadata(consignmentId: UUID, selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, FileMetadataValues]] =
    fileMetadataRepository.getFileMetadata(consignmentId, selectedFileIds).map { rows =>
      rows.groupBy(_.fileid).map { case (fileId, fileMetadata) =>
        fileId -> getFileMetadataValues(fileMetadata)
      }
    }

  private def getFileStatusRowForChecksumMatch(checksumFileMetadata: Option[FilemetadataRow], fileMetadataInput: FilemetadataRow): FilestatusRow = {
    val fileStatus = checksumFileMetadata match {
      case Some(cfm) if cfm.value == fileMetadataInput.value => Success
      case Some(_)                                           => Mismatch
      case None                                              => throw new IllegalStateException(s"Cannot find client side checksum for file ${fileMetadataInput.fileid}")
    }
    loggingUtils.logFileFormatStatus("checksumMatch", fileMetadataInput.fileid, fileStatus)
    FilestatusRow(uuidSource.uuid, fileMetadataInput.fileid, ChecksumMatch, fileStatus, fileMetadataInput.datetime)
  }

  private def getFileStatusRowForServerChecksum(fileMetadataInput: FilemetadataRow): FilestatusRow = {
    val fileStatus = fileMetadataInput.value match {
      case "" => Failed
      case _  => Success
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
  val Filename = "Filename"
  val FileType = "FileType"
  val FoiExemptionAsserted = "FoiExemptionAsserted"
  val TitleClosed = "TitleClosed"
  val DescriptionClosed = "DescriptionClosed"
  val ClosureType = "ClosureType"

  /** Save default values for these properties because TDR currently only supports records which are Open, in English, etc. Users agree to these conditions at a consignment level,
    * so it's OK to save these as defaults for every file. They need to be saved so they can be included in the export package. The defaults may be removed in future once we let
    * users upload a wider variety of records.
    */
  val RightsCopyright: StaticMetadata = StaticMetadata("RightsCopyright", "Crown Copyright")
  val LegalStatus: StaticMetadata = StaticMetadata("LegalStatus", "Public Record")
  val HeldBy: StaticMetadata = StaticMetadata("HeldBy", "TNA")
  val Language: StaticMetadata = StaticMetadata("Language", "English")
  val FoiExemptionCode: StaticMetadata = StaticMetadata("FoiExemptionCode", "open")
  val clientSideProperties: List[String] = List(SHA256ClientSideChecksum, ClientSideOriginalFilepath, ClientSideFileLastModifiedDate, ClientSideFileSize, Filename, FileType)

  def getFileMetadataValues(fileMetadataRow: Seq[FilemetadataRow]): FileMetadataValues = {
    val propertyNameMap: Map[String, String] = fileMetadataRow.groupBy(_.propertyname).transform { (_, value) =>
      value.head.value
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
      propertyNameMap.get(TitleClosed).map(_.toBoolean),
      propertyNameMap.get(DescriptionClosed).map(_.toBoolean)
    )
  }

  case class StaticMetadata(name: String, value: String)

  case class FileMetadataValue(name: String, value: String)

  case class File(
      fileId: UUID,
      fileType: Option[String] = None,
      fileName: Option[String] = None,
      parentId: Option[UUID] = None,
      metadata: FileMetadataValues,
      fileStatus: Option[String] = None,
      ffidMetadata: Option[FFIDMetadata],
      antivirusMetadata: Option[AntivirusMetadata],
      originalFilePath: Option[String] = None,
      fileMetadata: List[FileMetadataValue] = Nil
  )

  case class FileMetadataValues(
      sha256ClientSideChecksum: Option[String],
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
      titleClosed: Option[Boolean],
      descriptionClosed: Option[Boolean]
  )

  case class PropertyAction(updateActionType: String, propertyName: String, propertyValue: String, fileId: UUID, metadataId: UUID)

  case class FileMetadataDelete(fileIds: Set[UUID], propertyNamesToDelete: Set[String])

  case class PropertyUpdates(metadataToDelete: FileMetadataDelete, rowsToAdd: Seq[FilemetadataRow] = Seq(), rowsToUpdate: Map[String, FileMetadataUpdate] = Map())
}
