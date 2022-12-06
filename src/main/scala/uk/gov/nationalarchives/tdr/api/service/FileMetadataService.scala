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
import uk.gov.nationalarchives.tdr.api.service.FileStatusService.{Completed, _}
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.MapView
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
    case class FieldGroup(groupName: String, fields: Seq[CustomMetadataField])

    def toPropertyNames: Set[String] = fields.map(_.name).toSet

    def toCustomMetadataFieldGroups: Seq[FieldGroup] = {
      Seq(FieldGroup(ClosureMetadata, closureFields), FieldGroup(DescriptiveMetadata, descriptiveFields))
    }

    def closureFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryClosure") || f.propertyGroup.contains("OptionalClosure"))
    }

    def descriptiveFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryMetadata") || f.propertyGroup.contains("OptionalMetadata"))
    }

    def toValueDependenciesGroups: Seq[FieldGroup] = {
      fields
        .flatMap(f => {
          val values: List[CustomMetadataValues] = f.values
          values.map(v => {
            FieldGroup(v.value, v.dependencies)
          })
        })
    }
  }

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def getSumOfFileSizes(consignmentId: UUID): Future[Int] = fileMetadataRepository.getSumOfFileSizes(consignmentId)

  def getCustomMetadataValuesWithDefault: Future[Seq[FilepropertyvaluesRow]] = customMetadataPropertiesRepository.getCustomMetadataValuesWithDefault

  @deprecated("Use addFileMetadata(input: AddFileMetadataWithFileIdInput): Future[List[FileMetadataWithFileId]]")
  def addFileMetadata(addFileMetadataInput: AddFileMetadataWithFileIdInputValues, userId: UUID): Future[FileMetadataWithFileId] =
    addFileMetadata(AddFileMetadataWithFileIdInput(addFileMetadataInput :: Nil), userId).map(_.head)

  def addFileMetadata(input: AddFileMetadataWithFileIdInput, userId: UUID) = {
    fileMetadataRepository
      .getFileMetadataByProperty(input.metadataInputValues.map(_.fileId), SHA256ClientSideChecksum)
      .flatMap(metadataRows => {
        val existingMetadataMap: Map[UUID, Option[FilemetadataRow]] = metadataRows.groupBy(_.fileid).view.mapValues(_.headOption).toMap
        val (metadataRow, statusRows) = input.metadataInputValues
          .map(addFileMetadataInput => {
            val fileId = addFileMetadataInput.fileId
            val fileMetadataRow =
              FilemetadataRow(uuidSource.uuid, fileId, addFileMetadataInput.value, Timestamp.from(timeSource.now), userId, addFileMetadataInput.filePropertyName)
            val statusRows = fileMetadataRow.propertyname match {
              case SHA256ServerSideChecksum =>
                Seq(getFileStatusRowForChecksumMatch(existingMetadataMap.get(fileId).flatten, fileMetadataRow), getFileStatusRowForServerChecksum(fileMetadataRow))
              case _ => Nil
            }
            (fileMetadataRow, statusRows)
          })
          .unzip
        fileMetadataRepository
          .addChecksumMetadata(metadataRow, statusRows.flatten)
          .map(_.map(row => FileMetadataWithFileId(row.propertyname, row.fileid, row.value)).toList)
      })
      .recover(err => throw InputDataException(err.getMessage))
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
      _ <- updateCustomMetadataStatuses(uniqueFileIds.toSet, input.consignmentId, distinctPropertyNames)
      metadataPropertiesAdded = addedRows.map(r => { FileMetadata(r.propertyname, r.value) }).toSet
    } yield BulkFileMetadata(fileIds.toSeq, metadataPropertiesAdded.toSeq)
  }

  private def validateProperty(fileIds: Set[UUID], fieldToValidate: CustomMetadataField, existingProperties: Seq[FilemetadataRow]): Seq[FilePropertyState] = {
    val propertyToValidateName: String = fieldToValidate.name
    val valueDependenciesGroups = Seq(fieldToValidate).toValueDependenciesGroups

    fileIds
      .flatMap(id => {
        val allExistingFileProperties: Seq[FilemetadataRow] = existingProperties.filter(_.fileid == id)
        val existingPropertiesToValidate = allExistingFileProperties.filter(_.propertyname == propertyToValidateName)
        if (existingPropertiesToValidate.isEmpty) {
          None
        } else {
          existingPropertiesToValidate.map(existingProperty => {
            val valueDependencies = valueDependenciesGroups.filter(_.groupName == existingProperty.value).toSet
            // Validity test will need to change if multiple value fields require a set of dependencies for each value, eg
            // FOIExemptionCode 1 requires ClosurePeriod 1
            // FOIExemptionCode 2 requires ClosurePeriod 2 etc
            val valid: Boolean = valueDependencies.flatMap(_.fields.toPropertyNames).subsetOf(allExistingFileProperties.map(_.propertyname).toSet)
            FilePropertyState(id, propertyToValidateName, valid)
          })
        }
      })
      .toSeq
  }

  def updateCustomMetadataStatuses(fileIds: Set[UUID], consignmentId: UUID, updatedProperties: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(customMetadataFields.toPropertyNames))
    } yield {
      val fieldGroups: Seq[CustomMetadataFieldsHelper#FieldGroup] = customMetadataFields.toCustomMetadataFieldGroups
      val groupsProperties: Set[String] = fieldGroups.flatMap(_.fields.toPropertyNames).toSet

      if (!updatedProperties.subsetOf(groupsProperties)) {
        List()
      } else {
        val allFileStatuses = fieldGroups
          .flatMap(group => {
            val states = group.fields.flatMap(f => validateProperty(fileIds, f, existingMetadataProperties))

            val statelessStatuses = fileIds
              .filter(id => !states.map(_.fileId).contains(id))
              .map(id => {
                FilestatusRow(UUID.randomUUID(), id, group.groupName, NotEntered, Timestamp.from(timeSource.now))
              })

            val statuses = states
              .groupBy(_.fileId)
              .map(s => {
                val status: String = if (s._2.forall(_.valid == true)) Completed else Incomplete
                FilestatusRow(UUID.randomUUID(), s._1, group.groupName, status, Timestamp.from(timeSource.now))
              })

            statuses ++ statelessStatuses

          })
          .toList

        fileStatusRepository.deleteFileStatus(fileIds, allFileStatuses.map(_.statustype).toSet)
        fileStatusRepository.addFileStatuses(allFileStatuses)

        allFileStatuses
      }
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

  case class FilePropertyState(fileId: UUID, propertyName: String, valid: Boolean)
}
