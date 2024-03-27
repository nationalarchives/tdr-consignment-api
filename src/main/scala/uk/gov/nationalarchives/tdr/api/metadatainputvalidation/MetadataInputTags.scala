package uk.gov.nationalarchives.tdr.api.metadatainputvalidation

import sangria.execution.BeforeFieldResult
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{AddOrUpdateBulkFileMetadataInput, DeleteFileMetadataInput, UpdateBulkFileMetadataInput}
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

    val (inputFileIds: Seq[UUID], inputConsignmentId: UUID) = arg match {
      case updateInput: UpdateBulkFileMetadataInput           => (updateInput.fileIds, updateInput.consignmentId)
      case deleteInput: DeleteFileMetadataInput               => (deleteInput.fileIds, deleteInput.consignmentId)
      case addOrUpdateInput: AddOrUpdateBulkFileMetadataInput => (addOrUpdateInput.fileMetadata.map(_.fileId), addOrUpdateInput.consignmentId)
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
        case true => throw AuthorisationException(s"User '$userId' does not own the files they are trying to access")
        case _ if inputErrors(fileFields, inputFileIds, inputConsignmentId) =>
          throw InputDataException("Input contains directory id or contains non-existing file id or consignment id is incorrect")
        case _ => continue
      }
    }
  }

  private def inputErrors(fileFields: Seq[FileDetails], inputIds: Seq[UUID], inputConsignmentId: UUID): Boolean = {
    val existingFileIds = fileFields.map(_.fileId).toSet
    val consignmentIds = fileFields.map(_.consignmentId).toSet

    val consignmentIdMismatch = consignmentIds.size != 1 || !consignmentIds.contains(inputConsignmentId)
    lazy val fileIdsMismatch = inputIds.toSet.size > existingFileIds.size
    lazy val containsDirectory = fileFields.exists(_.fileType.contains(NodeType.directoryTypeIdentifier))
    consignmentIdMismatch || fileIdsMismatch || containsDirectory
  }
}
