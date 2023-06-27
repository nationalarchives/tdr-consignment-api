package uk.gov.nationalarchives.tdr.api.metadatainputvalidation

import sangria.execution.{BeforeFieldResult, Middleware, MiddlewareBeforeField, MiddlewareQueryContext}
import sangria.schema.Context
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext

import scala.concurrent.ExecutionContext

class MetadataInputValidator(implicit executionContext: ExecutionContext) extends Middleware[ConsignmentApiContext] with MiddlewareBeforeField[ConsignmentApiContext] {

  override type QueryVal = Unit
  override type FieldVal = Unit

  override def beforeQuery(context: MiddlewareQueryContext[ConsignmentApiContext, _, _]): QueryVal = ()

  override def afterQuery(queryVal: QueryVal, context: MiddlewareQueryContext[ConsignmentApiContext, _, _]): Unit = ()

  override def beforeField(
      queryVal: QueryVal,
      mctx: MiddlewareQueryContext[ConsignmentApiContext, _, _],
      ctx: Context[ConsignmentApiContext, _]
  ): BeforeFieldResult[ConsignmentApiContext, Unit] = {
    val validationList: Seq[BeforeFieldResult[ConsignmentApiContext, Unit]] = ctx.field.tags.map {
      case v: MetadataInputTag => {
        v.validate(ctx)
      }
      case _ => continue
    }

    validationList.headOption.getOrElse(continue)
  }
}
