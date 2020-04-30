package uk.gov.nationalarchives.tdr.api.consignmentstatevalidation

import sangria.execution.BeforeFieldResult
import sangria.schema.Context
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.AddFilesInput
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ValidationTag}

import scala.concurrent.{ExecutionContext, Future}

trait ConsignmentStateTag extends ValidationTag

object ValidateNoPreviousUploadForConsignment extends ConsignmentStateTag {
  override def validateAsync(ctx: Context[ConsignmentApiContext, _])
                       (implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]] = {
    val consignmentId = ctx.arg[AddFilesInput]("addFilesInput").consignmentId

    ctx.ctx.consignmentService.consignmentHasFiles(consignmentId).map {
      case false => continue
      case true => throw ConsignmentStateException("Upload already occurred for consignment: " + consignmentId)
    }
  }
}
