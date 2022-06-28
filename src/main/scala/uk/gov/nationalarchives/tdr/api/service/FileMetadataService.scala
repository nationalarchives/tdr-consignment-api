package uk.gov.nationalarchives.tdr.api.service

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import com.typesafe.scalalogging.Logger
import uk.gov.nationalarchives.Tables.{FileRow, FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileRepository}
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{
  AddBulkFileMetadataInput,
  AddFileMetadataWithFileIdInput,
  BulkFileMetadata,
  FileMetadata,
  FileMetadataWithFileId,
  SHA256ServerSideChecksum
}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

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
          _ <- Future(loggingUtils.logFileFormatStatus("checksum", fileMetadataRow.fileid, fileStatus))
          row <- fileMetadataRepository.addChecksumMetadata(fileMetadataRow, fileStatusRow)
        } yield FileMetadataWithFileId(fileMetadataRow.propertyname, row.fileid, row.value)
      } recover {
        case e: Throwable =>
          throw InputDataException(s"Could not find metadata for file ${fileMetadataRow.fileid}", Some(e))
      }
      case _ => Future.failed(InputDataException(s"${fileMetadataRow.propertyname} found. We are only expecting checksum updates for now"))
    }
  }

  def addBulkFileMetadata(addBulkFileMetadataInput: AddBulkFileMetadataInput, userId: UUID): Future[BulkFileMetadata] = {
    val fileMetadataProperties = addBulkFileMetadataInput.metadataProperties.distinct
    val timestamp = Timestamp.from(timeSource.now)
    val uniqueFileIds: Seq[UUID] = addBulkFileMetadataInput.fileIds.distinct
    for {
      fileRows <- fileRepository.getAllDescendants(uniqueFileIds)
      fileIds: Seq[UUID] = fileRows.collect{case fileRow if fileRow.filetype.get == "File" => fileRow.fileid}
      fileMetadataRows: Seq[FilemetadataRow] = for {
        fileId <- fileIds
        fileMetadata <- fileMetadataProperties
      } yield FilemetadataRow(uuidSource.uuid, fileId, fileMetadata.value, timestamp, userId, fileMetadata.filePropertyName)
      metadataRows <- fileMetadataRepository.addFileMetadata(fileMetadataRows)
      fileIdsAndMetadata = metadataRows.foldLeft((Set[UUID](), Set[FileMetadata]())) {
        (fileIdsAndMetadata, fileMetadataRow) => {
          (
            fileIdsAndMetadata._1 + fileMetadataRow.fileid,
            fileIdsAndMetadata._2 + FileMetadata(fileMetadataRow.propertyname, fileMetadataRow.value)
          )
        }
      }
    } yield BulkFileMetadata(fileIdsAndMetadata._1.toSeq, fileIdsAndMetadata._2.toSeq)
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

  case class StaticMetadata(name: String, value: String)

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

  def getFileMetadataValues(fileMetadataRow: Seq[FilemetadataRow]): FileMetadataValues = {
    val propertyNameMap: Map[String, String] = fileMetadataRow.groupBy(_.propertyname).transform{
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

}
