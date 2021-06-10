package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.time.LocalDateTime
import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasExportAccess, ValidateUserHasAccessToConsignment, ValidateUserOwnsFiles}
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ValidateNoPreviousUploadForConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.FileType
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.FileMetadataValues

object FileFields {
  case class Files(fileIds: Seq[UUID])
  case class FileDetails(fileId: UUID, consignmentId: UUID, userId: UUID, dateTime: LocalDateTime, checksumMatches: Option[Boolean])

  case class AddFilesInput(consignmentId: UUID, numberOfFiles: Int, parentFolder: String) extends UserOwnsConsignment
  implicit val AddFilesInputType: InputObjectType[AddFilesInput] = deriveInputObjectType[AddFilesInput]()
  implicit val FilesType: ObjectType[Unit, Files] = deriveObjectType[Unit, Files]()
  implicit val FileMetadataValuesType: ObjectType[Unit, FileMetadataValues] = deriveObjectType[Unit, FileMetadataValues]()

  private val FileInputArg = Argument("addFilesInput", AddFilesInputType)
  private val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  private val FileIdArg: Argument[UUID] = Argument("fileid", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "getFiles",
      FilesType,
      arguments = List(ConsignmentIdArg),
      resolve = ctx => ctx.ctx.fileService.getFiles(ctx.arg(ConsignmentIdArg)),
      tags=List(ValidateHasExportAccess)
    ),
    Field(
      "file",
      FileType,
      arguments = List(FileIdArg),
      resolve = ctx => ctx.ctx.fileService.getFile(ctx.arg(FileIdArg)),
      //Assumption that only if the user owns the files can they view the file details
      tags=List(ValidateUserOwnsFiles)
    )
  )

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFiles",
      FilesType,
      arguments = List(FileInputArg),
      resolve = ctx => ctx.ctx.fileService.addFile(ctx.arg(FileInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(FileInputArg), ValidateNoPreviousUploadForConsignment)
    )
  )
}
