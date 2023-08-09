package uk.gov.nationalarchives.tdr.api.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.alpakka.slick.javadsl.SlickSession
import sangria.ast.Document
import sangria.execution._
import sangria.marshalling.sprayJson._
import sangria.parser.QueryParser
import spray.json.{JsObject, JsString, JsValue}
import uk.gov.nationalarchives.tdr.api.auth.ValidationAuthoriser
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateValidator
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.graphql.{DeferredResolver, GraphQlTypes}
import uk.gov.nationalarchives.tdr.api.metadatainputvalidation.MetadataInputValidator
import uk.gov.nationalarchives.tdr.keycloak.Token

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class GraphQLServer(slickSession: SlickSession) extends GraphQLServerBase {

  def endpoint(requestJSON: JsValue, accessToken: Token)(implicit ec: ExecutionContext): Route = {

    val JsObject(fields) = requestJSON

    val JsString(query) = fields("query")

    QueryParser.parse(query) match {
      case Success(queryAst) =>
        val operation = fields.get("operationName") collect { case JsString(op) =>
          op
        }
        val variables = fields.get("variables") match {
          case Some(obj: JsObject) => obj
          case _                   => JsObject.empty
        }
        complete(executeGraphQLQuery(queryAst, operation, variables, accessToken))
      case Failure(error) =>
        complete(BadRequest, JsObject("error" -> JsString(error.getMessage)))
    }
  }

  private def executeGraphQLQuery(query: Document, operation: Option[String], vars: JsObject, accessToken: Token)(implicit
      ec: ExecutionContext
  ): Future[(StatusCode with Serializable, JsValue)] = {
    val context = generateConsignmentApiContext(accessToken: Token, DbConnection(slickSession).db)

    Executor
      .execute(
        GraphQlTypes.schema,
        query,
        context,
        variables = vars,
        operationName = operation,
        deferredResolver = new DeferredResolver,
        middleware = new ValidationAuthoriser :: new ConsignmentStateValidator :: new MetadataInputValidator :: Nil,
        exceptionHandler = exceptionHandler
      )
      .map(OK -> _)
      .recover {
        case error: QueryAnalysisError => BadRequest -> error.resolveError
        case error: ErrorWithResolver  => InternalServerError -> error.resolveError
      }
  }
}
object GraphQLServer {
  def apply(slickSession: SlickSession): GraphQLServer = new GraphQLServer(slickSession)
}
