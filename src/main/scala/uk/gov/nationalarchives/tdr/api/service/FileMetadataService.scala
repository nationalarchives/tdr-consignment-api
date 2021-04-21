package uk.gov.nationalarchives.tdr.api.service

import uk.gov
import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FileRow}
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{AddFileMetadataInput, FileMetadata, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._

import scala.concurrent.{ExecutionContext, Future}

class FileMetadataService(fileMetadataRepository: FileMetadataRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val ec: ExecutionContext) {

  def addStaticMetadata(files: Seq[FileRow], userId: UUID): Future[Seq[FilemetadataRow]] = {
    val now = Timestamp.from(timeSource.now)
    val fileMetadataRows = for {
      staticMetadata <- staticMetadataProperties
      fileId <- files.map(_.fileid)
    } yield FilemetadataRow(uuidSource.uuid, fileId, staticMetadata.value, now, userId, staticMetadata.name)
    fileMetadataRepository.addFileMetadata(fileMetadataRows)
  }

  def addFileMetadata(addFileMetadataInput: AddFileMetadataInput, userId: UUID): Future[FileMetadata] = {

    val filePropertyName = addFileMetadataInput.filePropertyName
    val row =
      FilemetadataRow(uuidSource.uuid, addFileMetadataInput.fileId,
        addFileMetadataInput.value,
        Timestamp.from(timeSource.now),
        userId, addFileMetadataInput.filePropertyName)

    filePropertyName match {
      case SHA256ServerSideChecksum =>
        (for {
          cfm <- fileMetadataRepository.getFileMetadata(addFileMetadataInput.fileId, SHA256ClientSideChecksum)
          row <- fileMetadataRepository.addChecksumMetadata(row, cfm.headOption.map(_.value == addFileMetadataInput.value))
        } yield FileMetadata(filePropertyName, row.fileid, row.value)) recover {
          case e: Throwable => throw InputDataException(s"Could not find metadata for file ${addFileMetadataInput.fileId}", Some(e))
        }
      case _ => Future.failed(InputDataException(s"$filePropertyName found. We are only expecting checksum updates for now"))
    }
  }

  def getFileMetadata(consignmentId: UUID): Future[Map[UUID, FileMetadataValues]] = fileMetadataRepository.getFileMetadata(consignmentId).map {
    rows =>
      rows.groupBy(_.fileid).map {
        case (fileId, fileMetadata) =>
          val propertyNameMap: Map[String, String] = fileMetadata.groupBy(_.propertyname)
            .transform((_, value) => value.head.value)
          fileId -> FileMetadataValues(
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

  case class File(fileId: UUID, metadata: FileMetadataValues, ffidMetadata: Option[FFIDMetadata])

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

}
