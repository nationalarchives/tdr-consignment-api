package uk.gov.nationalarchives.tdr.api.graphql

import sangria.execution.{BeforeFieldResult, FieldTag}
import sangria.schema.Context
import uk.gov.nationalarchives.tdr.api.auth.ValidationAuthoriser.{AuthorisationException, continue}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.Consignment
import uk.gov.nationalarchives.tdr.api.graphql.fields.TransferAgreementFields.AddTransferAgreementInput
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Tags {

  abstract class ValidateTags() extends FieldTag {
    def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit]
  }

  case class ValidateBody() extends ValidateTags {
    override def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
      val token = ctx.ctx.accessToken

      val isAdmin: Boolean = token.roles.contains("tdr_admin")

      val bodyArg: Option[String] = ctx.argOpt("body")

      if(!isAdmin) {
        val bodyFromToken: String = token.transferringBody.getOrElse("")
        if(bodyFromToken != bodyArg.getOrElse("")) {
          val msg = s"Body for user ${token.userId.getOrElse("")} was ${bodyArg.getOrElse("")} in the query and $bodyFromToken in the token"
          throw AuthorisationException(msg)
        }
        continue
      } else {
        continue
      }
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

  case class ValidateUserOwnsConsignment(argName: String) extends ValidateTags {
    override def validate(ctx: Context[ConsignmentApiContext, _]): BeforeFieldResult[ConsignmentApiContext, Unit] = {
      val token = ctx.ctx.accessToken
      val userId = token.userId.getOrElse("")

      // Consignment id is either inside a case class extending UserOwnsConsignment or is a Long argument
      val consignmentId: Long = Try(ctx.arg[UserOwnsConsignment](argName)) match {
        case Success(value) => value.consignmentId
        case Failure(_) => ctx.arg[Long](argName)
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
