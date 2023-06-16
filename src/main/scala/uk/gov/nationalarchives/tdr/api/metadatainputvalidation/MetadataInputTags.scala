package uk.gov.nationalarchives.tdr.api.metadatainputvalidation

import sangria.execution.BeforeFieldResult
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.UpdateBulkFileMetadataInput
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ValidationTag}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.auth.AuthorisationException
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException

import java.util.UUID
import scala.concurrent._
import scala.language.postfixOps

trait MetadataInputTag extends ValidationTag

case class ValidateMetadataInput[T](argument: Argument[T]) extends MetadataInputTag {

  override def validateAsync(ctx: Context[ConsignmentApiContext, _])(implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val arg: T = ctx.arg[T](argument.name)

    val fileIds: Seq[UUID] = arg match {
      case input: UpdateBulkFileMetadataInput => input.fileIds
    }

    val userId = ctx.ctx.accessToken.userId

    if (fileIds.isEmpty) {
      throw InputDataException(s"'fileIds' is empty. Please provide at least one fileId.")
    }

    for {
      fileFields <- ctx.ctx.fileService.getFileDetails(fileIds)
      ids = fileFields.map(_.fileId).toSet
      nonOwnership = fileFields.exists(_.userId != userId)
      containsDirectoryIds = fileFields.exists(_.fileType.contains(NodeType.directoryTypeIdentifier))
      missing = fileIds.toSet.size > ids.size
    } yield {
      nonOwnership match {
        case true                                 => throw AuthorisationException(s"User '$userId' does not own the files they are trying to access")
        case _ if containsDirectoryIds || missing => throw InputDataException("Input contains directory id or contains non-existing file")
        case _                                    => continue
      }
    }
  }
}
