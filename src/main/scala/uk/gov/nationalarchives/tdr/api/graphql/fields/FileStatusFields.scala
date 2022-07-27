package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, IntType, ListType, ObjectType, OptionType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.UuidType
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes.{UuidType, ZonedDateTimeType}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.FileSequenceType

import java.time.ZonedDateTime
import java.util.UUID

object FileStatusFields {

  case class FileStatus(fileStatusId: UUID,
                        fileId: UUID,
                               statusType: String,
                               value: String,
                               createdDatetime: ZonedDateTime,
                               modifiedDatetime: Option[ZonedDateTime])

  case class FileStatusInput(fileId: UUID,
                                    statusType: String,
                                    statusValue: String)

  case class AddFileStatusInput(consignmentId: UUID, statusInput: List[FileStatusInput]) extends UserOwnsConsignment

  implicit val FileStatusType: ObjectType[Unit, FileStatus] = deriveObjectType[Unit, FileStatus]()
  implicit val FileStatusInputType: InputObjectType[FileStatusInput] = deriveInputObjectType[FileStatusInput]()
  implicit val AddFileStatusInputType: InputObjectType[AddFileStatusInput] = deriveInputObjectType[AddFileStatusInput]()

  val AddFileStatusInputArg: Argument[AddFileStatusInput] = Argument("addFileStatusInput", AddFileStatusInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addFileStatuses", ListType(FileStatusType),
      arguments = AddFileStatusInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileStatusService.addFileStatuses(ctx.arg(AddFileStatusInputArg)),
      tags = List(ValidateUserHasAccessToConsignment(AddFileStatusInputArg))
    )
  )
}
