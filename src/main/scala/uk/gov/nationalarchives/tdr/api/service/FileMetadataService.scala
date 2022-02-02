package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.scalalogging.Logger

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import uk.gov.nationalarchives.Tables._
import uk.gov.nationalarchives.tdr.api.db.repository.FileMetadataRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.AntivirusMetadataFields.AntivirusMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FFIDMetadataFields.FFIDMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{AddFileMetadataInput, FileMetadata, SHA256ServerSideChecksum}
import uk.gov.nationalarchives.tdr.api.graphql.fields.MetadataFields._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.LoggingUtils

import scala.concurrent.{ExecutionContext, Future}

class FileMetadataService(fileMetadataRepository: FileMetadataRepository,
                          timeSource: TimeSource, uuidSource: UUIDSource)(implicit val ec: ExecutionContext) {

  val loggingUtils: LoggingUtils = LoggingUtils(Logger("FileMetadataService"))

  def getPropertyType(propertyType: Option[String]): PropertyType = propertyType match {
    case Some("System") => System
    case Some("Defined") => Defined
    case Some("Supplied") => Supplied
    case _ => throw new Exception(s"Invalid property type $propertyType")
  }

  def getDataType(dataType: Option[String]): DataType = dataType match {
    case Some("text") => Text
    case Some("datetime") => DateTime
    case Some("integer") => Integer
    case Some("decimal") => Decimal
    case _ => throw new Exception(s"Invalid data type $dataType")
  }

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
    val timestamp = Timestamp.from(timeSource.now)
    val fileMetadataRow =
      FilemetadataRow(uuidSource.uuid, addFileMetadataInput.fileId,
        addFileMetadataInput.value,
        timestamp,
        userId, addFileMetadataInput.filePropertyName)

    filePropertyName match {
      case SHA256ServerSideChecksum =>
        (for {
          cfm <- fileMetadataRepository.getFileMetadata(addFileMetadataInput.fileId, SHA256ClientSideChecksum)
          fileStatus: String = cfm.headOption match {
            case Some(cfm) if cfm.value == addFileMetadataInput.value => Success
            case Some(cfm) if cfm.value != addFileMetadataInput.value => Mismatch
            case None => throw new IllegalStateException(s"Cannot find client side checksum for file ${addFileMetadataInput.fileId}")
          }
          fileStatusRow: FilestatusRow = FilestatusRow(uuidSource.uuid, addFileMetadataInput.fileId, Checksum, fileStatus, timestamp)
          _ <- Future(loggingUtils.logFileFormatStatus("checksum", addFileMetadataInput.fileId, fileStatus))
          row <- fileMetadataRepository.addChecksumMetadata(fileMetadataRow, fileStatusRow)
        } yield FileMetadata(filePropertyName, row.fileid, row.value)) recover {
          case e: Throwable =>
            throw InputDataException(s"Could not find metadata for file ${addFileMetadataInput.fileId}", Some(e))
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

  def getClosureMetadata: Future[Seq[MetadataField]] = {
    (for {
      properties <- fileMetadataRepository.getClosureMetadataProperty
      values <- fileMetadataRepository.getClosureMetadataValues
      dependencies <- fileMetadataRepository.getClosureMetadataDependencies
    } yield (properties, values, dependencies)).map {
      case (properties, valuesResult, dependenciesResult) =>
        val values: Map[String, Seq[Filepropertyvaluesv2Row]] = valuesResult.groupBy(_.propertyname)
        val dependencies: Map[Int, Seq[Filepropertydependenciesv2Row]] = dependenciesResult.groupBy(_.groupid)

        def rowsToMetadata(fp: Filepropertyv2Row, defaultValueOption: Option[String] = None): MetadataField = {
          val metadataValues: Seq[MetadataValues] = values.getOrElse(fp.name, Nil).map(value => {
            value.dependencies.map(groupId => {
              val deps: Seq[MetadataField] = for {
                dep <- dependencies.getOrElse(groupId, Nil)
                dependencyProps <- properties.find(_.name == dep.propertyname).map(fp => {
                  rowsToMetadata(fp, dep.default)
                })
              } yield dependencyProps
              MetadataValues(deps.toList, value.propertyvalue)
            }).getOrElse(MetadataValues(Nil, value.propertyvalue))
          })
          MetadataField(
            fp.name,
            fp.fullname,
            fp.description,
            getPropertyType(fp.propertytype),
            fp.propertygroup,
            getDataType(fp.datatype),
            fp.editable.getOrElse(false),
            fp.mutlivalue.getOrElse(false),
            defaultValueOption,
            metadataValues.toList
          )
        }

        properties.map(prop => {
          val defaultValue: Option[String] = for {
            values <- values.get(prop.name)
            value <- values.find(_.default.getOrElse(false))
          } yield value.propertyvalue
          rowsToMetadata(prop, defaultValue)
        }).toList
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

  case class File(fileId: UUID, metadata: FileMetadataValues, ffidMetadata: Option[FFIDMetadata], antivirusMetadata: Option[AntivirusMetadata])

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
