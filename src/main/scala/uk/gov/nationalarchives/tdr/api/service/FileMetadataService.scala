package uk.gov.nationalarchives.tdr.api.service

import sangria.macros.derive.GraphQLDeprecated
import uk.gov.nationalarchives.Tables.FilemetadataRow
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.FileStatus
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.Reference
import uk.gov.nationalarchives.tdr.schemautils.ConfigUtils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataService(fileMetadataRepository: FileMetadataRepository)(implicit val ec: ExecutionContext) {

  def getSumOfFileSizes(consignmentId: UUID): Future[Long] = fileMetadataRepository.getSumOfFileSizes(consignmentId)

  def addFileMetadata(input: AddFileMetadataWithFileIdInput, userId: UUID): Future[List[FileMetadataWithFileId]] = {
    val metadataRow = input.metadataInputValues
      .map(addFileMetadataInput => {
        val fileId = addFileMetadataInput.fileId
        AddFileMetadataInput(fileId, addFileMetadataInput.value, userId, addFileMetadataInput.filePropertyName)
      })
    fileMetadataRepository
      .addFileMetadata(metadataRow)
      .map(_.map(row => FileMetadataWithFileId(row.propertyname, row.fileid, row.value)).toList)
      .recover(err => throw InputDataException(err.getMessage))
  }

  def addOrUpdateBulkFileMetadata(metadataInput: AddOrUpdateBulkFileMetadataInput, userId: UUID): Future[List[FileMetadataWithFileId]] = {
    val protectedMetadata = systemProperties.map(p => tdrDataLoadHeaderToPropertyMapper(p))
    metadataInput.fileMetadata.map { addOrUpdateFileMetadata =>
      addOrUpdateFileMetadata.metadata.map { metadata =>
        if (protectedMetadata.contains(metadata.filePropertyName)) {
          throw InputDataException(s"Protected metadata property found: ${metadata.filePropertyName}")
        }
      }
    }

    for {
      _ <- metadataInput.fileMetadata.map(fileMetadata => fileMetadataRepository.deleteFileMetadata(fileMetadata.fileId, fileMetadata.metadata.map(_.filePropertyName).toSet)).head
      addedRows <- fileMetadataRepository.addFileMetadata(generateFileMetadataInput(metadataInput.fileMetadata, userId))
      metadataPropertiesAdded = addedRows.map(r => FileMetadataWithFileId(r.propertyname, r.fileid, r.value)).toList
    } yield metadataPropertiesAdded
  }

  private def generateFileMetadataInput(fileMetadata: Seq[AddOrUpdateFileMetadata], userId: UUID): List[AddFileMetadataInput] = {
    (for {
      addOrUpdateFileMetadata <- fileMetadata
      addOrUpdateMetadata <- addOrUpdateFileMetadata.metadata
      if addOrUpdateMetadata.value.nonEmpty
    } yield AddFileMetadataInput(addOrUpdateFileMetadata.fileId, addOrUpdateMetadata.value, userId, addOrUpdateMetadata.filePropertyName)).toList
  }

  def getFileMetadata(consignmentId: Option[UUID], selectedFileIds: Option[Set[UUID]] = None): Future[Map[UUID, FileMetadataValues]] =
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
  val FileUUID = "UUID"
  val Filename = "Filename"
  val FileType = "FileType"
  val FileReference = "FileReference"
  val ParentReference = "ParentReference"
  val FoiExemptionAsserted = "FoiExemptionAsserted"
  val TitleClosed = "TitleClosed"
  val DescriptionClosed = "DescriptionClosed"
  val ClosureType = "ClosureType"
  val Description = "description"
  val DescriptionAlternate = "DescriptionAlternate"
  val RightsCopyright = "RightsCopyright"
  val LegalStatus = "LegalStatus"
  val HeldBy = "HeldBy"
  val Language = "Language"
  val FoiExemptionCode = "FoiExemptionCode"
  val EvidenceProvidedBy = "EvidenceProvidedBy"
  val clientSideProperties: List[String] =
    List(SHA256ClientSideChecksum, ClientSideOriginalFilepath, ClientSideFileLastModifiedDate, ClientSideFileSize, Filename, FileType)

  def getFileMetadataValues(fileMetadataRow: Seq[FilemetadataRow]): FileMetadataValues = {
    val propertyNameMap: Map[String, String] = fileMetadataRow.groupBy(_.propertyname).transform { (_, value) =>
      value.head.value
    }
    FileMetadataValues(
      propertyNameMap.get(SHA256ClientSideChecksum),
      propertyNameMap.get(ClientSideOriginalFilepath),
      propertyNameMap.get(ClientSideFileLastModifiedDate).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(ClientSideFileSize).map(_.toLong),
      propertyNameMap.get(RightsCopyright),
      propertyNameMap.get(LegalStatus),
      propertyNameMap.get(HeldBy),
      propertyNameMap.get(Language),
      propertyNameMap.get(FoiExemptionCode),
      propertyNameMap.get(ClosurePeriod),
      propertyNameMap.get(ClosureStartDate).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(FoiExemptionAsserted).map(d => Timestamp.valueOf(d).toLocalDateTime),
      propertyNameMap.get(TitleClosed).map(_.toBoolean),
      propertyNameMap.get(DescriptionClosed).map(_.toBoolean),
      propertyNameMap.get(EvidenceProvidedBy)
    )
  }

  case class FileMetadataValue(name: String, value: String)

  case class AddFileMetadataInput(fileId: UUID, value: String, userId: UUID, filePropertyName: String)

  case class File(
      fileId: UUID,
      uploadMatchId: Option[String] = None,
      fileType: Option[String] = None,
      fileName: Option[String] = None,
      fileReference: Option[Reference],
      parentId: Option[UUID] = None,
      parentReference: Option[Reference],
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
      closurePeriod: Option[String],
      closureStartDate: Option[LocalDateTime],
      foiExemptionAsserted: Option[LocalDateTime],
      titleClosed: Option[Boolean],
      descriptionClosed: Option[Boolean],
      evidenceProvidedBy: Option[String]
  )

  val config: ConfigUtils.MetadataConfiguration = ConfigUtils.loadConfiguration
  private val systemProperties: Seq[Reference] = config.getPropertiesByPropertyType("System")
  private val tdrDataLoadHeaderToPropertyMapper: String => String = config.propertyToOutputMapper("tdrDataLoadHeader")
}
