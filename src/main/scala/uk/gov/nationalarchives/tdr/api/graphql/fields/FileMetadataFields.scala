package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasChecksumMetadataAccess, ValidateUserOwnsFiles}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import FieldTypes._

object FileMetadataFields {
  trait FileMetadataBase {
    val filePropertyName: String
    val value: String
  }

  val SHA256ServerSideChecksum = "SHA256ServerSideChecksum"
  case class FileMetadata(filePropertyName: String, value: String) extends FileMetadataBase
  case class AddFileMetadataInput(filePropertyName: String, value: String) extends FileMetadataBase

  case class FileMetadataWithFileId(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase
  case class BulkFileMetadata(fileIds: Seq[UUID], metadataProperties: Seq[FileMetadata])
  case class AddFileMetadataWithFileIdInput(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase
  case class UpdateBulkFileMetadataInput(consignmentId: UUID, fileIds: Seq[UUID], metadataProperties: Seq[AddFileMetadataInput])

  implicit val FileMetadataType: ObjectType[Unit, FileMetadata] = deriveObjectType[Unit, FileMetadata]()
  implicit val InputFileMetadataType: InputObjectType[AddFileMetadataInput] = deriveInputObjectType[AddFileMetadataInput]()

  implicit val FileMetadataWithFileIdType: ObjectType[Unit, FileMetadataWithFileId] = deriveObjectType[Unit, FileMetadataWithFileId]()
  implicit val AddFileMetadataInputType: InputObjectType[AddFileMetadataWithFileIdInput] = deriveInputObjectType[AddFileMetadataWithFileIdInput]()

  val BulkFileMetadataType: ObjectType[Unit, BulkFileMetadata] = deriveObjectType[Unit, BulkFileMetadata]()
  val AddElseUpdateBulkFileMetadataInputType: InputObjectType[UpdateBulkFileMetadataInput] = deriveInputObjectType[UpdateBulkFileMetadataInput]()

  implicit val FileMetadataWithFileIdInputArg: Argument[AddFileMetadataWithFileIdInput] = Argument("addFileMetadataWithFileIdInput", AddFileMetadataInputType)
  implicit val BulkFileMetadataInputArg: Argument[UpdateBulkFileMetadataInput] =
    Argument("input", AddElseUpdateBulkFileMetadataInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addFileMetadata", FileMetadataWithFileIdType,
      arguments=FileMetadataWithFileIdInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.addFileMetadata(ctx.arg(FileMetadataWithFileIdInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateHasChecksumMetadataAccess)
    ),
    Field("updateBulkFileMetadata", BulkFileMetadataType,
      arguments=BulkFileMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.updateBulkFileMetadata(ctx.arg(BulkFileMetadataInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserOwnsFiles)
    )
  )
}
