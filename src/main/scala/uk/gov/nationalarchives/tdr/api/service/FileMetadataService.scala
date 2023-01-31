package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import sangria.macros.derive.GraphQLDeprecated
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.FileStatus
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
    customMetadataService: CustomMetadataPropertiesService,
    validateFileMetadataService: ValidateFileMetadataService,
    timeSource: TimeSource,
    uuidSource: UUIDSource
)(implicit val ec: ExecutionContext) {

  implicit class FileRowsHelper(fileRows: Seq[FileRow]) {
    def toFileTypeIds: Set[UUID] = {
      fileRows.collect { case fileRow if fileRow.filetype.get == NodeType.fileTypeIdentifier => fileRow.fileid }.toSet
    }
  }

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def getSumOfFileSizes(consignmentId: UUID): Future[Int] = fileMetadataRepository.getSumOfFileSizes(consignmentId)

  @deprecated("Use addFileMetadata(input: AddFileMetadataWithFileIdInput): Future[List[FileMetadataWithFileId]]")
  def addFileMetadata(addFileMetadataInput: AddFileMetadataWithFileIdInputValues, userId: UUID): Future[FileMetadataWithFileId] =
    addFileMetadata(AddFileMetadataWithFileIdInput(addFileMetadataInput :: Nil), userId).map(_.head)

  def addFileMetadata(input: AddFileMetadataWithFileIdInput, userId: UUID): Future[List[FileMetadataWithFileId]] = {
    val metadataRow = input.metadataInputValues
      .map(addFileMetadataInput => {
        val fileId = addFileMetadataInput.fileId
        FilemetadataRow(uuidSource.uuid, fileId, addFileMetadataInput.value, Timestamp.from(timeSource.now), userId, addFileMetadataInput.filePropertyName)
      })
    fileMetadataRepository
      .addFileMetadata(metadataRow)
      .map(_.map(row => FileMetadataWithFileId(row.propertyname, row.fileid, row.value)).toList)
      .recover(err => throw InputDataException(err.getMessage))
  }

  def updateBulkFileMetadata(input: UpdateBulkFileMetadataInput, userId: UUID): Future[BulkFileMetadata] = {
    val emptyPropertyValues: Seq[String] = input.metadataProperties.filter(_.value.isEmpty).map(_.filePropertyName)

    if (emptyPropertyValues.nonEmpty) {
      throw InputDataException(s"Cannot update properties with empty value: ${emptyPropertyValues.mkString(", ")}")
    }

    val distinctMetadataProperties: Set[UpdateFileMetadataInput] = input.metadataProperties.toSet
    val distinctPropertyNames: Set[String] = distinctMetadataProperties.map(_.filePropertyName)
    val uniqueFileIds: Seq[UUID] = input.fileIds.distinct

    for {
      existingFileRows <- fileRepository.getAllDescendants(uniqueFileIds)
      fileIds: Set[UUID] = existingFileRows.toFileTypeIds
      _ <- fileMetadataRepository.deleteFileMetadata(fileIds, distinctPropertyNames)
      addedRows <- fileMetadataRepository.addFileMetadata(generateFileMetadataRows(fileIds, distinctMetadataProperties, userId))
      _ <- validateFileMetadataService.validateAdditionalMetadata(uniqueFileIds.toSet, input.consignmentId, distinctPropertyNames)
      metadataPropertiesAdded = addedRows.map(r => { FileMetadata(r.propertyname, r.value) }).toSet
    } yield BulkFileMetadata(fileIds.toSeq, metadataPropertiesAdded.toSeq)
  }

  def deleteFileMetadata(input: DeleteFileMetadataInput, userId: UUID): Future[DeleteFileMetadata] = {
    val propertiesToDelete = descriptionDeletionHandler(input.propertyNames)
    for {
      existingFileRows <- fileRepository.getAllDescendants(input.fileIds.distinct)
      fileIds: Set[UUID] = existingFileRows.toFileTypeIds
      customMetadataProperties <- customMetadataService.getCustomMetadata
      allPropertiesToDelete: Set[String] = customMetadataProperties
        .collect {
          case customMetadataProperty if propertiesToDelete.contains(customMetadataProperty.name) =>
            val namesOfDependenciesToDelete: List[String] = customMetadataProperty.values.flatMap(_.dependencies.map(_.name))
            namesOfDependenciesToDelete :+ customMetadataProperty.name
        }
        .flatten
        .toSet

      _ = if (allPropertiesToDelete.isEmpty) {
        throw new IllegalStateException(
          s"Can't find metadata property '${input.propertyNames.mkString(" or ")}' in the db"
        )
      }

      propertyDefaults: Seq[(String, String)] = customMetadataProperties.collect {
        case customMetadataProperty if allPropertiesToDelete.contains(customMetadataProperty.name) && customMetadataProperty.defaultValue.nonEmpty =>
          (customMetadataProperty.name, customMetadataProperty.defaultValue.get)
      }

      metadataToReset: Seq[FilemetadataRow] = fileIds.flatMap { id =>
        propertyDefaults.map { case (propertyName, defaultValue) =>
          FilemetadataRow(uuidSource.uuid, id, defaultValue, Timestamp.from(timeSource.now), userId, propertyName)
        }
      }.toSeq
      _ <- fileMetadataRepository.deleteFileMetadata(fileIds, allPropertiesToDelete)
      _ <- fileMetadataRepository.addFileMetadata(metadataToReset)
      _ <- validateFileMetadataService.validateAdditionalMetadata(fileIds, existingFileRows.map(_.consignmentid).head, allPropertiesToDelete)
    } yield DeleteFileMetadata(fileIds.toSeq, allPropertiesToDelete.toSeq)
  }

  private def descriptionDeletionHandler(originalPropertyNames: Seq[String]): Seq[String] = {
    // Ensure that the file metadata is returned to the correct state if the 'description' property is deleted
    // Cannot have a 'DescriptionAlternate' property without a 'description' property
    // 'DescriptionAlternate' property is a dependency of 'DescriptionClosed' property
    // If 'description' is deleted then 'DescriptionClosed' property to be set back to default of 'false' and 'DescriptionAlternate' to be deleted
    if (originalPropertyNames.contains(Description)) {
      originalPropertyNames ++ Set(DescriptionClosed)
    } else originalPropertyNames
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
}

object FileMetadataService {

  val SHA256ClientSideChecksum = "SHA256ClientSideChecksum"
  val ClientSideOriginalFilepath = "ClientSideOriginalFilepath"
  val OriginalFilepath = "OriginalFilepath"
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
  val Description = "description"
  val DescriptionAlternate = "DescriptionAlternate"

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
      @GraphQLDeprecated("Should use 'fileStatuses' field")
      fileStatus: Option[String] = None,
      ffidMetadata: Option[FFIDMetadata],
      antivirusMetadata: Option[AntivirusMetadata],
      originalFilePath: Option[String] = None,
      fileMetadata: List[FileMetadataValue] = Nil,
      fileStatuses: List[FileStatus] = Nil
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
}
