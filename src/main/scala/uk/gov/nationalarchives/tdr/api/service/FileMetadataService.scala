package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilepropertyRow, FilepropertydependenciesRow, FilepropertyvaluesRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{CustomMetadataPropertiesRepository, FileMetadataRepository, FileMetadataUpdate, FileRepository}
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

class FileMetadataService(
    fileMetadataRepository: FileMetadataRepository,
    fileRepository: FileRepository,
    customMetadataPropertiesRepository: CustomMetadataPropertiesRepository,
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
      metadataPropertiesAdded = addedRows.map(r => { FileMetadata(r.propertyname, r.value) }).toSet
    } yield BulkFileMetadata(fileIds.toSeq, metadataPropertiesAdded.toSeq)
  }

  def deleteFileMetadata(input: DeleteFileMetadataInput, userId: UUID): Future[DeleteFileMetadata] = {
    val getAllDescendants: Future[Seq[FileRow]] = fileRepository.getAllDescendants(input.fileIds.distinct)
    val getCustomMetadataProperty: Future[Seq[FilepropertyRow]] = customMetadataPropertiesRepository.getCustomMetadataProperty
    val getCustomMetadataValues: Future[Seq[FilepropertyvaluesRow]] = customMetadataPropertiesRepository.getCustomMetadataValues
    val getCustomMetadataDependencies: Future[Seq[FilepropertydependenciesRow]] = customMetadataPropertiesRepository.getCustomMetadataDependencies

    for {
      existingFileRows <- getAllDescendants
      fileIds: Set[UUID] = existingFileRows.toFileTypeIds
      allMetadataProperties <- getCustomMetadataProperty
      metadataValues <- getCustomMetadataValues
      allDependencies <- getCustomMetadataDependencies
      propertiesToDelAndDefaultToAddBack: Map[String, Option[FileMetadataUpdate]] =
        input.propertyNamesAndValues.flatMap { propertyNamesAndValue =>
          val valueToDelete: Option[String] = propertyNamesAndValue.valueToDelete
          val propertyNameToDelete: String = propertyNamesAndValue.filePropertyName

          val valuesBelongingToProperty: Seq[FilepropertyvaluesRow] =
            metadataValues.filter(metadataValue => metadataValue.propertyname == propertyNameToDelete)

          val filePropertyValueRowsToDelete: Seq[FilepropertyvaluesRow] =
            if (valueToDelete.nonEmpty) {
              val filePropertyValueRowOfValueToDel: Seq[FilepropertyvaluesRow] =
                valuesBelongingToProperty.filter(property => valueToDelete.contains(property.propertyvalue))
              if (filePropertyValueRowOfValueToDel.isEmpty) {
                throw new IllegalStateException(
                  s"Can't find metadata property '$propertyNameToDelete' with value '$valueToDelete' in the db"
                )
              } else {
                filePropertyValueRowOfValueToDel
              }
            } else {
              // if value to delete has not been passed, still check if property has values that have dependencies
              // because you shouldn't be able to delete a property without deleting all of its dependencies
              valuesBelongingToProperty
            }

          getFileMetadataToDeleteAndReset(
            userId,
            allMetadataProperties,
            metadataValues,
            allDependencies,
            propertyNameToDelete,
            valuesBelongingToProperty,
            filePropertyValueRowsToDelete
          )
        }.toMap

      fileMetadataToDelete: Seq[String] = propertiesToDelAndDefaultToAddBack.keys.toSeq
      defaultPropertiesToAddBack: Seq[FilemetadataRow] = propertiesToDelAndDefaultToAddBack.values.flatten.flatMap { defaultProperty =>
        fileIds.map { fileId =>
          FilemetadataRow(
            UUID.randomUUID(),
            fileId,
            defaultProperty.value,
            defaultProperty.dateTime,
            defaultProperty.userId,
            defaultProperty.filePropertyName
          )
        }
      }.toSeq

      _ <- fileMetadataRepository.deleteFileMetadata(fileIds, fileMetadataToDelete.toSet)
      _ <- fileMetadataRepository.addFileMetadata(defaultPropertiesToAddBack)
    } yield DeleteFileMetadata(fileIds.toSeq, fileMetadataToDelete)
  }

  private def getFileMetadataToDeleteAndReset(
      userId: UUID,
      allMetadataProperties: Seq[FilepropertyRow],
      metadataValues: Seq[FilepropertyvaluesRow],
      allDependencies: Seq[FilepropertydependenciesRow],
      propertyNameToDelete: String,
      valuesBelongingToProperty: Seq[FilepropertyvaluesRow],
      filePropertyValueRowOfValueToDel: Seq[FilepropertyvaluesRow]
  ): Seq[(String, Option[FileMetadataUpdate])] = {
    val propertyExists: Boolean = allMetadataProperties.exists(property => property.name == propertyNameToDelete)
    if (!propertyExists) { throw new IllegalArgumentException(s"'$propertyNameToDelete' is not an existing property.") }

    val potentialFileMetadataRowToSetAsDefault: Option[FilepropertyvaluesRow] = valuesBelongingToProperty.find(_.default.contains(true))
    val propertyNameAndValuesToDeleteOrSetAsDefault: Seq[(String, Option[FileMetadataUpdate])] = potentialFileMetadataRowToSetAsDefault match {
      case Some(fileMetadataRowToSetAsDefault) =>
        val defaultFileMetadataToAddToProperty: FileMetadataUpdate =
          FileMetadataUpdate(Nil, fileMetadataRowToSetAsDefault.propertyname, fileMetadataRowToSetAsDefault.propertyvalue, Timestamp.from(timeSource.now), userId)
        Seq(propertyNameToDelete -> Some(defaultFileMetadataToAddToProperty))
      case None =>
        Seq(propertyNameToDelete -> None)
    }

    val groupIds: Seq[Int] = filePropertyValueRowOfValueToDel.flatMap(_.dependencies)
    val dependenciesToDelete: Seq[String] = allDependencies.collect { case dependency if groupIds.contains(dependency.groupid) => dependency.propertyname }

    val dependencyNameAndValuesToDeleteOrSetAsDefault: Seq[(String, Option[FileMetadataUpdate])] = dependenciesToDelete.flatMap { dependencyToDelete =>
      val valuesBelongingToProperty: Seq[FilepropertyvaluesRow] = metadataValues.filter(metadataValue => metadataValue.propertyname == dependencyToDelete)
      getFileMetadataToDeleteAndReset(
        userId,
        allMetadataProperties,
        metadataValues,
        allDependencies,
        dependencyToDelete,
        valuesBelongingToProperty,
        valuesBelongingToProperty
      )
    }

    propertyNameAndValuesToDeleteOrSetAsDefault ++ dependencyNameAndValuesToDeleteOrSetAsDefault
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

  case class FileMetadataValueDeletions(valuesToReset: Seq[FileMetadataUpdate], valuesToDelete: Seq[String])

  case class PropertyUpdates(metadataToDelete: FileMetadataDelete, rowsToAdd: Seq[FilemetadataRow] = Seq(), rowsToUpdate: Map[String, FileMetadataUpdate] = Map())
}
