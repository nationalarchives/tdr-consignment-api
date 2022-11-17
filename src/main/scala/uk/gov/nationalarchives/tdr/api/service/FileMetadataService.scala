package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.CustomMetadataField
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

    def closureFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryClosure") || f.propertyGroup.contains("OptionalClosure"))
    }

    def descriptiveFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryMetadata") || f.propertyGroup.contains("OptionalMetadata"))
    }

    def dependencyFields: Seq[CustomMetadataField] = {
      fields.flatMap(_.values.flatMap(_.dependencies))
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
      // _ <- validateMetadata(uniqueFileIds.toSet, input.consignmentId, distinctPropertyNames)
      metadataPropertiesAdded = addedRows.map(r => { FileMetadata(r.propertyname, r.value) }).toSet
    } yield BulkFileMetadata(fileIds.toSeq, metadataPropertiesAdded.toSeq)
  }

  def generateMetadataStatuses(fileIds: Set[UUID], existingFileProperties: Seq[FilemetadataRow], dependencies: Set[String], statusType: String): List[FilestatusRow] = {
    fileIds
      .map(id => {
        val existingProperties = existingFileProperties.filter(_.fileid == id).map(_.propertyname).toSet
        // if all the dependencies exist as an existing property for the file then metadata is complete
        val statusValue = if (dependencies.subsetOf(existingProperties)) "Complete" else "InComplete"
        FilestatusRow(UUID.randomUUID(), id, statusType, statusValue, Timestamp.from(timeSource.now))
      })
      .toList
  }

  def validateMetadata(fileIds: Set[UUID], consignmentId: UUID, updatedProperties: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingFileProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(customMetadataFields.toPropertyNames))
    } yield {
      val closureProperties: Set[String] = customMetadataFields.closureFields.toPropertyNames
      val descriptiveProperties: Set[String] = customMetadataFields.descriptiveFields.toPropertyNames

      val containsClosure: Boolean = updatedProperties.exists(closureProperties.contains)
      val containsDescriptive: Boolean = updatedProperties.exists(descriptiveProperties.contains)

      val allClosureDependencies = customMetadataFields.closureFields.dependencyFields.toPropertyNames
      val allDescriptiveDependencies = customMetadataFields.descriptiveFields.dependencyFields.toPropertyNames

      val closureFileStatuses: List[FilestatusRow] = if (containsClosure) {
        generateMetadataStatuses(fileIds, existingFileProperties, allClosureDependencies, "ClosureMetadata")
      } else List()

      val descriptiveFileStatuses: List[FilestatusRow] = if (containsDescriptive) {
        generateMetadataStatuses(fileIds, existingFileProperties, allDescriptiveDependencies, "DescriptiveMetadata")
      } else List()

      val allFileStatuses: List[FilestatusRow] = closureFileStatuses ++ descriptiveFileStatuses

      // Only reset statuses if closure or descriptive properties are being updated
      if (allFileStatuses.nonEmpty) {
        val deletions: Set[String] = allFileStatuses.map(_.statustype).toSet
        fileStatusRepository.deleteFileStatus(fileIds, deletions)
        fileStatusRepository.addFileStatuses(allFileStatuses)
      }

      allFileStatuses
    }
  }

  def deleteFileMetadata(input: DeleteFileMetadataInput, userId: UUID): Future[DeleteFileMetadata] = {
    // Need to update the closure metadata status to "NotEntered" here aswell
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
  val DescriptionClosed = "DescriptionPublic"
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
      (propertyNameMap.get(DescriptionClosed) match {
        case Some(value) => Some(value)
        case None        => propertyNameMap.get("DescriptionClosed")
      }).map(_.toBoolean)
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
