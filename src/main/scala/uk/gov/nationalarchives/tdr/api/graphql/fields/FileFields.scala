package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.FileType

import java.util.UUID

object FileFields {

  case class FileMatches(fileId: UUID, matchId: Long)

  case class ClientSideMetadataInput(originalPath: String,
                                     checksum: String,
                                     lastModified: Long,
                                     fileSize: Long,
                                     matchId: Long)
  case class AddFileAndMetadataInput(consignmentId: UUID,
                                     metadataInput: List[ClientSideMetadataInput],
                                     emptyDirectories: List[String] = Nil) extends UserOwnsConsignment

  case class AllDescendantsInput(consignmentId: UUID, parentIds: List[UUID]) extends UserOwnsConsignment

  implicit val MetadataInputType: InputObjectType[ClientSideMetadataInput] = deriveInputObjectType[ClientSideMetadataInput]()
  implicit val AddFileAndMetadataInputType: InputObjectType[AddFileAndMetadataInput] = deriveInputObjectType[AddFileAndMetadataInput]()
  implicit val AllDescendantsInputType: InputObjectType[AllDescendantsInput] = deriveInputObjectType[AllDescendantsInput]()
  implicit val FileSequenceType: ObjectType[Unit, FileMatches]  = deriveObjectType[Unit, FileMatches]()
  private val FileAndMetadataInputArg = Argument("addFilesAndMetadataInput", AddFileAndMetadataInputType)
  private val AllDescendantsInputArg = Argument("allDescendantsInput", AllDescendantsInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFilesAndMetadata",
      ListType(FileSequenceType),
      arguments = List(FileAndMetadataInputArg),
      resolve = ctx => ctx.ctx.fileService.addFile(ctx.arg(FileAndMetadataInputArg), ctx.ctx.accessToken.userId),
      tags=List(ValidateUserHasAccessToConsignment(FileAndMetadataInputArg))
    )
  )

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("allDescendants",
      ListType(FileType),
      arguments = AllDescendantsInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileService.getAllDescendants(ctx.arg(AllDescendantsInputArg)),
      tags = List(ValidateUserHasAccessToConsignment(AllDescendantsInputArg))
    ))
}
