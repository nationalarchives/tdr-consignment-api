package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasChecksumMetadataAccess, ValidateUserOwnsFiles}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import FieldTypes._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.ClosureType

object FileMetadataFields {
  trait FileMetadataBase {
    val filePropertyName: String
    val value: String
  }

  val SHA256ServerSideChecksum = "SHA256ServerSideChecksum"

  case class FileMetadata(filePropertyName: String, value: String) extends FileMetadataBase
  case class UpdateFileMetadataInput(filePropertyIsMultiValue: Boolean, filePropertyName: String, value: String) extends FileMetadataBase
  // Option[String] instead of String in case you want to delete all values of property or in case value does not have properties

  case class FileMetadataWithFileId(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase

  case class BulkFileMetadata(fileIds: Seq[UUID], metadataProperties: Seq[FileMetadata])

  case class AddFileMetadataWithFileIdInput(metadataInputValues: List[AddFileMetadataWithFileIdInputValues])

  case class AddFileMetadataWithFileIdInputValues(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase

  case class UpdateBulkFileMetadataInput(consignmentId: UUID, fileIds: Seq[UUID], metadataProperties: Seq[UpdateFileMetadataInput])

  case class DeleteFileMetadata(fileIds: Seq[UUID], filePropertyNames: Seq[String])

  case class DeleteFileMetadataInput(
      fileIds: Seq[UUID],
      propertyNames: Seq[String] = Seq(ClosureType) // temporary until we can update it
  )

  implicit val FileMetadataType: ObjectType[Unit, FileMetadata] = deriveObjectType[Unit, FileMetadata]()
  implicit val InputFileMetadataType: InputObjectType[UpdateFileMetadataInput] = deriveInputObjectType[UpdateFileMetadataInput]()

  implicit val FileMetadataWithFileIdType: ObjectType[Unit, FileMetadataWithFileId] = deriveObjectType[Unit, FileMetadataWithFileId]()
  implicit val AddFileMetadataInputValuesType: InputObjectType[AddFileMetadataWithFileIdInputValues] = deriveInputObjectType[AddFileMetadataWithFileIdInputValues]()
  implicit val AddFileMetadataInputType: InputObjectType[AddFileMetadataWithFileIdInput] = deriveInputObjectType[AddFileMetadataWithFileIdInput]()

  implicit val DeleteFileMetadataType: ObjectType[Unit, DeleteFileMetadata] = deriveObjectType[Unit, DeleteFileMetadata]()
  val DeleteFileMetadataInputType: InputObjectType[DeleteFileMetadataInput] = deriveInputObjectType[DeleteFileMetadataInput]()

  val BulkFileMetadataType: ObjectType[Unit, BulkFileMetadata] = deriveObjectType[Unit, BulkFileMetadata]()
  val UpdateBulkFileMetadataInputType: InputObjectType[UpdateBulkFileMetadataInput] = deriveInputObjectType[UpdateBulkFileMetadataInput]()

  implicit val FileMetadataWithFileIdInputValuesArg: Argument[AddFileMetadataWithFileIdInputValues] = Argument("addFileMetadataWithFileIdInput", AddFileMetadataInputValuesType)
  implicit val FileMetadataWithFileIdInputArg: Argument[AddFileMetadataWithFileIdInput] = Argument("addMultipleFileMetadataInput", AddFileMetadataInputType)
  implicit val BulkFileMetadataInputArg: Argument[UpdateBulkFileMetadataInput] =
    Argument("updateBulkFileMetadataInput", UpdateBulkFileMetadataInputType)
  implicit val DeleteFileMetadataInputArg: Argument[DeleteFileMetadataInput] = Argument("deleteFileMetadataInput", DeleteFileMetadataInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFileMetadata",
      FileMetadataWithFileIdType,
      arguments = FileMetadataWithFileIdInputValuesArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.addFileMetadata(ctx.arg(FileMetadataWithFileIdInputValuesArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateHasChecksumMetadataAccess)
    ),
    Field(
      "addMultipleFileMetadata",
      ListType(FileMetadataWithFileIdType),
      arguments = FileMetadataWithFileIdInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.addFileMetadata(ctx.arg(FileMetadataWithFileIdInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateHasChecksumMetadataAccess)
    ),
    Field(
      "updateBulkFileMetadata",
      BulkFileMetadataType,
      arguments = BulkFileMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.updateBulkFileMetadata(ctx.arg(BulkFileMetadataInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateUserOwnsFiles(BulkFileMetadataInputArg))
    ),
    Field(
      "deleteFileMetadata",
      DeleteFileMetadataType,
      arguments = DeleteFileMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.deleteFileMetadata(ctx.arg(DeleteFileMetadataInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateUserOwnsFiles(DeleteFileMetadataInputArg))
    )
  )
}
