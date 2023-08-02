package uk.gov.nationalarchives.tdr.api.http

import cats.effect.IO
import io.circe.Json
import sangria.ast.Document
import sangria.execution._
import sangria.marshalling.circe._
import slick.jdbc.JdbcBackend
import uk.gov.nationalarchives.tdr.api.auth.ValidationAuthoriser
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateValidator
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, DeferredResolver, GraphQlTypes}
import uk.gov.nationalarchives.tdr.keycloak.Token

import scala.concurrent.ExecutionContext

class GraphQLServerHttp4s() extends GraphQLServerBase {

  def executeGraphQLQuery(query: Document, operation: Option[String], vars: Json, accessToken: Token, database: JdbcBackend#DatabaseDef)(implicit
      ec: ExecutionContext
  ): IO[Json] = {
    val context: IO[ConsignmentApiContext] = IO(generateConsignmentApiContext(accessToken: Token, database))
    context.flatMap { ctx =>
      IO.fromFuture(
        IO(
          Executor.execute(
            GraphQlTypes.schema,
            query,
            ctx,
            variables = vars,
            operationName = operation,
            deferredResolver = new DeferredResolver,
            middleware = new ValidationAuthoriser :: new ConsignmentStateValidator :: Nil,
            exceptionHandler = exceptionHandler
          )
        )
      ).recover { case error: QueryAnalysisError =>
        error.resolveError
      }
    }
  }

}

object GraphQLServerHttp4s {
  def apply(): GraphQLServerHttp4s = new GraphQLServerHttp4s()
}
