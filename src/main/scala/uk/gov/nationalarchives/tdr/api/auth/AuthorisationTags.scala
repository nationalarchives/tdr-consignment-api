package uk.gov.nationalarchives.tdr.api.auth

import java.util.UUID

import sangria.execution.BeforeFieldResult
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.AddConsignmentInput
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ValidationTag}
import uk.gov.nationalarchives.tdr.api.model.consignment.ConsignmentType.ConsignmentTypeHelper

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

    if(bodyFromToken != bodyArg) {
      val msg = s"Body for user ${token.userId} was $bodyArg in the query and $bodyFromToken in the token"
      throw AuthorisationException(msg)
    }
    continue
  }
}

object ValidateConsignmentCreation extends AuthorisationTag {

  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                       (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val token = ctx.ctx.accessToken
    val userId = token.userId
    val userBody = token.transferringBody.getOrElse(
      throw AuthorisationException(s"No transferring body in user token for user '$userId'"))
    val addConsignmentInput = ctx.arg[AddConsignmentInput]("addConsignmentInput")
    val seriesId: Option[UUID] = addConsignmentInput.seriesid
    val consignmentType: String = addConsignmentInput.consignmentType

    seriesId match {
      case Some(value) =>
        val bodyResult = ctx.ctx.transferringBodyService.getBody(value)
        bodyResult.map(body => {
          body.tdrCode match {
            case code if code == userBody => continue
            case code =>
              val message = s"User '$userId' is from transferring body '$userBody' and does not have permission " +
                s"to create a consignment under series '$value' owned by body '$code'"
              throw AuthorisationException(message)
          }
        })
      case _ if token.isJudgmentUser && consignmentType.isJudgment =>
        Future(continue)
      case _ =>
        val message = if (!token.isJudgmentUser) {
            s"User '$userId' is not a judgment user and does not have permission to create a consignment without a series"
          } else {
            s"Cannot create consignment without series for consignment type '$consignmentType'"
          }

        throw AuthorisationException(message)
    }
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

object ValidateHasReportingAccess extends SyncAuthorisationTag {
  override def validateSync(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val token = ctx.ctx.accessToken
    val reportingAccess = token.reportingRoles.contains(reportingRole)
    if (reportingAccess) {
      continue
    } else {
      val tokenUserId = token.userId
      throw AuthorisationException(s"User $tokenUserId does not have permission to run reporting")
    }
  }
}
