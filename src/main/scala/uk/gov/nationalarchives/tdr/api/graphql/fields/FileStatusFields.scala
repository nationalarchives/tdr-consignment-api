package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.generic.auto._
import sangria.macros.derive.{deriveInputObjectType, deriveObjectType}
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import FieldTypes._
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserOwnsFiles

import java.util.UUID

object FileStatusFields {

  case class FileStatus(fileId: UUID, statusType: String, statusValue: String)

  case class AddFileStatusInput(fileId: UUID, statusType: String, statusValue: String)
  case class AddMultipleFileStatusInput(statuses: List[AddFileStatusInput])

  implicit val FileStatusType: ObjectType[Unit, FileStatus] = deriveObjectType[Unit, FileStatus]()
  implicit val AddFileStatusInputType: InputObjectType[AddFileStatusInput] = deriveInputObjectType[AddFileStatusInput]()
  implicit val AddMultipleFileStatusInputType: InputObjectType[AddMultipleFileStatusInput] = deriveInputObjectType[AddMultipleFileStatusInput]()

  implicit val FileStatusInputArg: Argument[AddFileStatusInput] = Argument("addFileStatusInput", AddFileStatusInputType)
  implicit val MultipleFileStatusInputArg: Argument[AddMultipleFileStatusInput] = Argument("addMultipleFileStatusInput", AddMultipleFileStatusInputType)

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addFileStatus",
      FileStatusType,
      arguments = FileStatusInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileStatusService.addFileStatus(ctx.arg(FileStatusInputArg)),
      tags = List(ValidateUserOwnsFiles(FileStatusInputArg))
    ),
    Field(
      "addMultipleFileStatus",
      ListType(FileStatusType),
      arguments = MultipleFileStatusInputArg :: Nil,
      resolve = ctx => ctx.ctx.fileStatusService.addFileStatuses(ctx.arg(MultipleFileStatusInputArg)),
      tags = List(ValidateUserOwnsFiles(MultipleFileStatusInputArg))
    )
  )
}
