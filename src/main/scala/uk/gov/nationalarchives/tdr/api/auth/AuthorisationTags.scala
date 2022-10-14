package uk.gov.nationalarchives.tdr.api.auth

import sangria.execution.BeforeFieldResult
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserOwnsFiles.continue
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentFilters, UpdateConsignmentSeriesIdInput}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.{BulkFileMetadataInputArg, DeleteFileMetadataInputArg}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.FileStatusInputArg
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ValidationTag}
import uk.gov.nationalarchives.tdr.api.service.FileService.FileOwnership

import java.util.UUID
import scala.concurrent._
import scala.language.postfixOps

trait AuthorisationTag extends ValidationTag {
  val antiVirusRole = "antivirus"
  val checksumRole = "checksum"
  val clientFileMetadataRole = "client_file_metadata"
  val fileFormatRole = "file_format"
  val exportRole = "export"
  val reportingRole = "reporting"
}

trait SyncAuthorisationTag extends AuthorisationTag {
  final def validateAsync(ctx: Context[ConsignmentApiContext, _])
                         (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    Future.successful(validateSync(ctx))
  }

  def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit]
}

object ValidateBody extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken

    val bodyArg: String = ctx.arg("body")
    val bodyFromToken: String = token.transferringBody.getOrElse("")

    if (bodyFromToken != bodyArg) {
      val msg = s"Body for user ${token.userId} was $bodyArg in the query and $bodyFromToken in the token"
      throw AuthorisationException(msg)
    }
    continue
  }
}

object ValidateUpdateConsignmentSeriesId extends AuthorisationTag {

  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                            (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val token = ctx.ctx.accessToken
    val userId = token.userId
    val userBody = token.transferringBody.getOrElse(
      throw AuthorisationException(s"No transferring body in user token for user '$userId'"))
    val consignmentSeriesInput = ctx.arg[UpdateConsignmentSeriesIdInput]("updateConsignmentSeriesId")
    val seriesId: UUID = consignmentSeriesInput.seriesId
    val consignmentId: UUID = consignmentSeriesInput.consignmentId
    if (token.isJudgmentUser) {
      val message = "Judgment users cannot update series id"
      throw AuthorisationException(message)
    }

    val bodyResult = ctx.ctx.transferringBodyService.getBody(seriesId)
    bodyResult.map(body => {
      body.tdrCode match {
        case code if code == userBody => continue
        case code =>
          val message = s"User '$userId' is from transferring body '$userBody' and does not have permission " +
            s"to update a consignment '$consignmentId' under series '$seriesId' owned by body '$code'"
          throw AuthorisationException(message)
      }
    })
  }
}

case class ValidateUserHasAccessToConsignment[T](argument: Argument[T]) extends AuthorisationTag {
  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                            (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val token = ctx.ctx.accessToken
    val userId = token.userId
    val exportAccess = token.backendChecksRoles.contains(exportRole)

    val arg: T = ctx.arg[T](argument.name)
    val consignmentId: UUID = arg match {
      case uoc: UserOwnsConsignment => uoc.consignmentId
      case id: UUID => id
    }

    ctx.ctx.consignmentService
      .getConsignment(consignmentId)
      .map(consignment => {
        if (consignment.isDefined && (consignment.get.userid == userId || exportAccess)) {
          continue
        } else {
          throw AuthorisationException(s"User '$userId' does not have access to consignment '$consignmentId'")
        }
      })
  }
}

object ValidateHasAntiVirusMetadataAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val antivirusAccess = token.backendChecksRoles.contains(antiVirusRole)

    if (antivirusAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User '$tokenUserId' does not have permission to update antivirus metadata")
    }
  }
}

object ValidateHasChecksumMetadataAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val checksumAccess = token.backendChecksRoles.contains(checksumRole)

    if (checksumAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User '$tokenUserId' does not have permission to update checksum metadata")
    }
  }
}

object ValidateHasClientFileMetadataAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val clientFileMetadataAccess = token.backendChecksRoles.contains(clientFileMetadataRole)
    val fileId = ctx.arg[UUID]("fileId")

    if (clientFileMetadataAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User '$tokenUserId' does not have permission to access the client file metadata for file $fileId")
    }
  }
}

object ValidateHasFFIDMetadataAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val fileFormatAccess = token.backendChecksRoles.contains(fileFormatRole)
    if (fileFormatAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User '$tokenUserId' does not have permission to update file format metadata")
    }
  }
}

object ValidateHasExportAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val exportAccess = token.backendChecksRoles.contains(exportRole)
    if (exportAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User '$tokenUserId' does not have permission to export the files")
    }
  }
}

object ValidateUserOwnsFiles extends AuthorisationTag {
  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                            (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val addBulkMetadataInput = ctx.arg(BulkFileMetadataInputArg)
    val fileIds = addBulkMetadataInput.fileIds.toSeq

    ValidateFiles.validate(ctx, fileIds)
  }
}

object ValidateUserOwnsFilesForFileStatusInput extends AuthorisationTag {
  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                            (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val addFileStatusInput = ctx.arg(FileStatusInputArg)
    val fileIds = Seq(addFileStatusInput.fileId)

    ValidateFiles.validate(ctx, fileIds)
  }
}

object ValidateUserOwnsFilesForDeleteMetadataInput extends AuthorisationTag {
  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                            (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val deleteFileMetadataInput = ctx.arg(DeleteFileMetadataInputArg)
    val fileIds = deleteFileMetadataInput.fileIds

    ValidateFiles.validate(ctx, fileIds)
  }
}

object ValidateFiles {

  def validate(ctx: Context[ConsignmentApiContext, _], fileIds: Seq[UUID])
              (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val userId = ctx.ctx.accessToken.userId

    if (fileIds.isEmpty) {
      throw InputDataException(s"'fileIds' is empty. Please provide at least one fileId.")
    }
    for {
      fileIdsAndOwner: Seq[FileOwnership] <- ctx.ctx.fileService.getOwnersOfFiles(fileIds)
      fileIdsBelongingToAConsignment: Seq[UUID] = fileIdsAndOwner.map(_.fileId)
      filesThatDoNotBelongToAConsignment: Seq[UUID] = fileIds.filterNot(fileId => fileIdsBelongingToAConsignment.contains(fileId))
      fileIdsThatDoNotBelongToTheUser: Seq[UUID] = fileIdsAndOwner.collect {
        case fileIdAndOwner if fileIdAndOwner.userId != userId => fileIdAndOwner.fileId
      }
      allFilesBelongToAConsignment = filesThatDoNotBelongToAConsignment.isEmpty
      allFilesBelongToTheUser = fileIdsThatDoNotBelongToTheUser.isEmpty
      result = if (allFilesBelongToAConsignment && allFilesBelongToTheUser) {
        continue
      } else {
        val fileIdsNotOwnedByUser: Seq[UUID] = filesThatDoNotBelongToAConsignment ++ fileIdsThatDoNotBelongToTheUser
        throw AuthorisationException(s"User '$userId' does not own the files they are trying to access:\n${fileIdsNotOwnedByUser.mkString("\n")}")
      }
    } yield result
  }
}

object ValidateHasConsignmentsAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val consignmentFilters: Option[ConsignmentFilters] = ctx.args.argOpt("consignmentFiltersInput")
    val token = ctx.ctx.accessToken
    val reportingAccess = token.reportingRoles.contains(reportingRole)
    if (reportingAccess || consignmentFilters.exists(_.userId.contains(token.userId))) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User $tokenUserId does not have permission to access the consignments")
    }
  }
}
