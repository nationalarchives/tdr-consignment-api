package uk.gov.nationalarchives.tdr.api.metadatainputvalidation

import sangria.execution.BeforeFieldResult
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.UpdateBulkFileMetadataInput
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ValidationTag}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.auth.AuthorisationException
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.service.FileService.FileDetails

import java.util.UUID
import scala.concurrent._
import scala.language.postfixOps

trait MetadataInputTag extends ValidationTag

case class ValidateMetadataInput[T](argument: Argument[T]) extends MetadataInputTag {

  override def validateAsync(ctx: Context[ConsignmentApiContext, _])(implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val arg: T = ctx.arg[T](argument.name)

    val inputFileIds: Seq[UUID] = arg match {
      case input: UpdateBulkFileMetadataInput => input.fileIds
    }

    val userId = ctx.ctx.accessToken.userId

    if (inputFileIds.isEmpty) {
      throw InputDataException(s"'fileIds' is empty. Please provide at least one fileId.")
    }

    for {
      fileFields <- ctx.ctx.fileService.getFileDetails(inputFileIds)
      nonOwnership = fileFields.exists(_.userId != userId)
    } yield {
      nonOwnership match {
        case true                                       => throw AuthorisationException(s"User '$userId' does not own the files they are trying to access")
        case _ if inputErrors(fileFields, inputFileIds) => throw InputDataException("Input contains directory id or contains non-existing file")
        case _                                          => continue
      }
    }
  }

  private def inputErrors(fileFields: Seq[FileDetails], inputIds: Seq[UUID]): Boolean = {
    val existingIds = fileFields.map(_.fileId).toSet
    inputIds.toSet.size > existingIds.size || fileFields.exists(_.fileType.contains(NodeType.directoryTypeIdentifier))
  }
}
