package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, OptionInputType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasConsignmentsAccess, ValidateUserHasAccessToConsignment}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.validation.{ServiceTransfer, UserOwnsConsignment}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.{UuidType, ZonedDateTimeType}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.FileType

import java.time.ZonedDateTime
import java.util.UUID

object FileFields {

  case class FileMatches(fileId: UUID, matchId: String)
  case class FileCheckFailure(
      fileId: UUID,
      consignmentId: UUID,
      consignmentType: String,
      rankOverFilePath: Int,
      PUID: Option[String],
      userId: UUID,
      statusType: String,
      statusValue: String,
      seriesName: Option[String],
      transferringBodyName: Option[String],
      antivirusResult: Option[String],
      extension: Option[String],
      identificationBasis: Option[String],
      extensionMismatch: Boolean,
      formatName: Option[String],
      checksum: Option[String],
      createdDateTime: ZonedDateTime
  )

  case class ClientSideMetadataInput(originalPath: String, checksum: String, lastModified: Long, fileSize: Long, matchId: String)
  case class AddFileAndMetadataInput(consignmentId: UUID, metadataInput: List[ClientSideMetadataInput], emptyDirectories: List[String] = Nil, userIdOverride: Option[UUID] = None)
      extends UserOwnsConsignment
      with ServiceTransfer
  case class GetFileCheckFailuresInput(
      consignmentId: Option[UUID] = None,
      startDateTime: Option[ZonedDateTime] = None,
      endDateTime: Option[ZonedDateTime] = None
  )

  implicit val MetadataInputType: InputObjectType[ClientSideMetadataInput] = deriveInputObjectType[ClientSideMetadataInput]()
  implicit val AddFileAndMetadataInputType: InputObjectType[AddFileAndMetadataInput] = deriveInputObjectType[AddFileAndMetadataInput]()
  implicit val FileSequenceType: ObjectType[Unit, FileMatches] = deriveObjectType[Unit, FileMatches]()
  implicit val GetFileCheckFailuresInputType: InputObjectType[GetFileCheckFailuresInput] = deriveInputObjectType[GetFileCheckFailuresInput]()
  implicit val FileCheckFailureType: ObjectType[Unit, FileCheckFailure] = deriveObjectType[Unit, FileCheckFailure]()

  private val FileAndMetadataInputArg = Argument("addFilesAndMetadataInput", AddFileAndMetadataInputType)
  private val GetFileCheckFailuresInputArg = Argument("getFileCheckFailuresInput", OptionInputType(GetFileCheckFailuresInputType))

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFilesAndMetadata",
      ListType(FileSequenceType),
      arguments = List(FileAndMetadataInputArg),
      resolve = ctx => ctx.ctx.fileService.addFile(ctx.arg(FileAndMetadataInputArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(FileAndMetadataInputArg))
    )
  )

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      name = "getFileCheckFailures",
      fieldType = ListType(FileCheckFailureType),
      arguments = List(GetFileCheckFailuresInputArg),
      resolve = ctx => ctx.ctx.fileService.getFileCheckFailures(ctx.arg(GetFileCheckFailuresInputArg)),
      tags = List(ValidateHasConsignmentsAccess)
    )
  )
}
