package uk.gov.nationalarchives.tdr.api.graphql

import java.util.UUID

import sangria.execution.{BeforeFieldResult, FieldTag}
import sangria.schema.{Argument, Context}
import uk.gov.nationalarchives.tdr.api.auth.ValidationAuthoriser.{AuthorisationException, continue}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.Consignment
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

object Tags {

  abstract class ValidateTags() extends FieldTag {
    def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit]
  }

  case class ValidateBody() extends ValidateTags {
    override def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
      val token = ctx.ctx.accessToken

      val bodyArg: String = ctx.arg("body")
      val bodyFromToken: String = token.transferringBody.getOrElse("")

      if(bodyFromToken != bodyArg) {
        val msg = s"Body for user ${token.userId.getOrElse("")} was ${bodyArg} in the query and $bodyFromToken in the token"
        throw AuthorisationException(msg)
      }
      continue
    }
  }

  case class ValidateIsAdmin() extends ValidateTags {
    override def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
      val token = ctx.ctx.accessToken
      val isAdmin: Boolean = token.roles.contains("tdr_admin")
      if(isAdmin) {
        continue
      } else {
        throw AuthorisationException(s"Admin permissions required to call ${ctx.field.name}")
      }
    }
  }

  case class ValidateUserOwnsConsignment[T](argument: Argument[T]) extends ValidateTags {
    override def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
      val token = ctx.ctx.accessToken
      val userId = token.userId.getOrElse("")

      val arg: T = ctx.arg[T](argument.name)
      val consignmentId: UUID = arg match {
        case uoc: UserOwnsConsignment => uoc.consignmentId
        case id: UUID => id
      }

      val result = ctx.ctx.consignmentService.getConsignment(consignmentId)

      val consignment: Option[Consignment] = Await.result(result, 5 seconds)

      if(consignment.isEmpty) {
        throw AuthorisationException("Invalid consignment id")
      }

      if (consignment.get.userid.toString == userId) {
        continue
      } else {
        throw AuthorisationException("User does not own consignment")
      }
    }
  }
}
