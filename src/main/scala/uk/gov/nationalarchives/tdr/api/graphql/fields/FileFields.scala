package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID
import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasExportAccess, ValidateUserHasAccessToConsignment}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import uk.gov.nationalarchives.tdr.api.utils.TreeNodesUtils.TreeNode

object FileFields {
  case class Files(fileIds: Seq[UUID])
  case class FileMatches(fileId: UUID, matchId: Long)

  case class ClientSideMetadataInput(originalPath: String,
                                     checksum: String,
                                     lastModified: Long,
                                     fileSize: Long,
                                     matchId: Long)
  case class AddFileAndMetadataInput(consignmentId: UUID, metadataInput: List[ClientSideMetadataInput]) extends UserOwnsConsignment
  implicit val TreeNodeType: ObjectType[Unit, TreeNode] = deriveObjectType[Unit, TreeNode]()
  implicit val MetadataInputType: InputObjectType[ClientSideMetadataInput] = deriveInputObjectType[ClientSideMetadataInput]()
  implicit val AddFileAndMetadataInputType: InputObjectType[AddFileAndMetadataInput] = deriveInputObjectType[AddFileAndMetadataInput]()
  implicit val FileType: ObjectType[Unit, Files]  = deriveObjectType[Unit, Files]()
  implicit val FileSequenceType: ObjectType[Unit, FileMatches]  = deriveObjectType[Unit, FileMatches]()
  private val FileAndMetadataInputArg = Argument("addFilesAndMetadataInput", AddFileAndMetadataInputType)
  private val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  private val FileIdArg: Argument[UUID] = Argument("fileId", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "getChildren",
      ListType(TreeNodeType),
      arguments = FileIdArg :: Nil,
      resolve = ctx => ctx.ctx.fileService.getChildren(ctx.arg(FileIdArg)),
      tags=List()
    )
  )

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFilesAndMetadata",
      ListType(FileSequenceType),
      arguments = List(FileAndMetadataInputArg),
      resolve = ctx => ctx.ctx.fileService.addFile(ctx.arg(FileAndMetadataInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(FileAndMetadataInputArg))
    )
  )
}
