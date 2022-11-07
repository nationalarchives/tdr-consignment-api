package uk.gov.nationalarchives.tdr.api.graphql

import sangria.execution.{BeforeFieldResult, FieldTag}
import sangria.schema.Context
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.concurrent.{Await, ExecutionContext, Future}

trait ValidationTag extends FieldTag {
  def validate(ctx: Context[ConsignmentApiContext, _])(implicit executionContext: ExecutionContext): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val validationResult = validateAsync(ctx)

    // Awaiting a Future is risky because the thread will block until the response is returned or the timeout is reached.
    // It could cause the API to be slow because akka-http cannot assign threads to new requests while this one is
    // blocked.
    //
    // We are only using Await because Sangria middleware does not support Futures like the main resolvers do. We should
    // remove it when we find a way to do authorisation in a completely async way in Sangria.
    Await.result(validationResult, 120 seconds)
  }

  def validateAsync(ctx: Context[ConsignmentApiContext, _])(implicit executionContext: ExecutionContext): Future[BeforeFieldResult[ConsignmentApiContext, Unit]]

  val continue: BeforeFieldResult[ConsignmentApiContext, Unit] = BeforeFieldResult(())
}
