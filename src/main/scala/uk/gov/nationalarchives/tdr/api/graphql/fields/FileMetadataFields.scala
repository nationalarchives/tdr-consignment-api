package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateHasChecksumMetadataAccess
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.metadatainputvalidation.ValidateMetadataInput
import FieldTypes._

object FileMetadataFields {
  trait FileMetadataBase {
    val filePropertyName: String
    val value: String
  }

  val SHA256ServerSideChecksum = "SHA256ServerSideChecksum"

  case class FileMetadata(filePropertyName: String, value: String) extends FileMetadataBase

  case class FileMetadataWithFileId(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase

  case class AddFileMetadataWithFileIdInput(metadataInputValues: List[AddFileMetadataWithFileIdInputValues])

  case class AddFileMetadataWithFileIdInputValues(filePropertyName: String, fileId: UUID, value: String) extends FileMetadataBase

  case class AddOrUpdateMetadata(filePropertyName: String, value: String) extends FileMetadataBase
  case class AddOrUpdateFileMetadata(fileId: UUID, metadata: Seq[AddOrUpdateMetadata])
  case class AddOrUpdateBulkFileMetadataInput(consignmentId: UUID, fileMetadata: Seq[AddOrUpdateFileMetadata], skipValidation: Boolean = false)

  implicit val FileMetadataType: ObjectType[Unit, FileMetadata] = deriveObjectType[Unit, FileMetadata]()

  implicit val FileMetadataWithFileIdType: ObjectType[Unit, FileMetadataWithFileId] = deriveObjectType[Unit, FileMetadataWithFileId]()
  implicit val AddFileMetadataInputValuesType: InputObjectType[AddFileMetadataWithFileIdInputValues] = deriveInputObjectType[AddFileMetadataWithFileIdInputValues]()
  implicit val AddFileMetadataInputType: InputObjectType[AddFileMetadataWithFileIdInput] = deriveInputObjectType[AddFileMetadataWithFileIdInput]()

  implicit val InputAddOrUpdateMetadataType: InputObjectType[AddOrUpdateMetadata] = deriveInputObjectType[AddOrUpdateMetadata]()
  implicit val InputAddOrUpdateFileMetadataType: InputObjectType[AddOrUpdateFileMetadata] = deriveInputObjectType[AddOrUpdateFileMetadata]()

  val AddOrUpdateBulkFileMetadataInputType: InputObjectType[AddOrUpdateBulkFileMetadataInput] = deriveInputObjectType[AddOrUpdateBulkFileMetadataInput]()

  implicit val FileMetadataWithFileIdInputValuesArg: Argument[AddFileMetadataWithFileIdInputValues] = Argument("addFileMetadataWithFileIdInput", AddFileMetadataInputValuesType)
  implicit val FileMetadataWithFileIdInputArg: Argument[AddFileMetadataWithFileIdInput] = Argument("addMultipleFileMetadataInput", AddFileMetadataInputType)
  implicit val AddOrUpdateBulkFileMetadataInputArg: Argument[AddOrUpdateBulkFileMetadataInput] =
    Argument("addOrUpdateBulkFileMetadataInput", AddOrUpdateBulkFileMetadataInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addMultipleFileMetadata",
      ListType(FileMetadataWithFileIdType),
      arguments = FileMetadataWithFileIdInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.addFileMetadata(ctx.arg(FileMetadataWithFileIdInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateHasChecksumMetadataAccess)
    ),
    Field(
      "addOrUpdateBulkFileMetadata",
      ListType(FileMetadataWithFileIdType),
      arguments = AddOrUpdateBulkFileMetadataInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileMetadataService.addOrUpdateBulkFileMetadata(ctx.arg(AddOrUpdateBulkFileMetadataInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateMetadataInput(AddOrUpdateBulkFileMetadataInputArg))
    )
  )
}
