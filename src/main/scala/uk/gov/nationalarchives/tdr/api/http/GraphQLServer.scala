package uk.gov.nationalarchives.tdr.api.http

import cats.effect.IO
import cats.implicits._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import io.circe.Json
import sangria.ast.Document
import sangria.execution._
import sangria.marshalling.ResultMarshaller
import sangria.marshalling.circe._
import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.DatabaseDef
import uk.gov.nationalarchives.tdr.api.auth.{AuthorisationException, ValidationAuthoriser}
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.{ConsignmentStateException, ConsignmentStateValidator}
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, DeferredResolver, ErrorCodes, GraphQlTypes}
import uk.gov.nationalarchives.tdr.api.service._
import uk.gov.nationalarchives.tdr.keycloak.Token

import scala.concurrent.ExecutionContext

class GraphQLServer() {

  private val logger = Logger(s"${GraphQLServer.getClass}")
  private val config = ConfigFactory.load()

  private def handleException(marshaller: ResultMarshaller, errorCode: String, message: String): HandledException = {
    val node = marshaller.scalarNode(errorCode, "String", Set.empty)
    val additionalFields = Map("code" -> node)
    logger.warn(s"$message, Code: $errorCode")
    HandledException(message, additionalFields)
  }

  val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case (resultMarshaller, AuthorisationException(message)) =>
      handleException(resultMarshaller, ErrorCodes.notAuthorised, message)
    case (resultMarshaller, ConsignmentStateException(message)) =>
      handleException(resultMarshaller, ErrorCodes.invalidConsignmentState, message)
    case (resultMarshaller, InputDataException(message, _)) =>
      handleException(resultMarshaller, ErrorCodes.invalidInputData, message)
    // Sangria catches all unhandled exceptions and returns a response. We'll rethrow them here so Akka can deal with them.
    case (_, ex: Throwable) => throw ex
  }

  //scalastyle:off method.length
  private def generateConsignmentApiContext(accessToken: Token, db: JdbcBackend#DatabaseDef)(implicit ec: ExecutionContext): IO[ConsignmentApiContext] = {
    val uuidSourceClass: Class[_] = Class.forName(config.getString("source.uuid"))
    val uuidSource: UUIDSource = uuidSourceClass.getDeclaredConstructor().newInstance().asInstanceOf[UUIDSource]
    val timeSource = new CurrentTimeSource
    val consignmentRepository = new ConsignmentRepository(db, timeSource)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val fileRepository = new FileRepository(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val ffidMetadataMatchesRepository = new FFIDMetadataMatchesRepository(db)
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val antivirusMetadataRepository = new AntivirusMetadataRepository(db)
    val disallowedPuidsRepository = new DisallowedPuidsRepository(db)
    val allowedPuidsRepository = new AllowedPuidsRepository(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val transferringBodyService = new TransferringBodyService(new TransferringBodyRepository(db))
    val consignmentService = new ConsignmentService(consignmentRepository, consignmentStatusRepository, fileMetadataRepository, fileRepository,
      ffidMetadataRepository, transferringBodyService, timeSource, uuidSource, config)
    val seriesService = new SeriesService(new SeriesRepository(db), uuidSource)
    val transferAgreementService = new TransferAgreementService(new ConsignmentMetadataRepository(db), new ConsignmentStatusRepository(db),
      uuidSource, timeSource)
    val finalTransferConfirmationService = new FinalTransferConfirmationService(new ConsignmentMetadataRepository(db), consignmentStatusRepository,
      uuidSource, timeSource)
    val clientFileMetadataService = new ClientFileMetadataService(fileMetadataRepository)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepository, uuidSource, timeSource)
    val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
    val fileMetadataService = new FileMetadataService(fileMetadataRepository, fileRepository, customMetadataPropertiesRepository, timeSource, uuidSource)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepository, ffidMetadataMatchesRepository, fileRepository,
      allowedPuidsRepository, disallowedPuidsRepository, timeSource, uuidSource)
    val fileStatusService = new FileStatusService(fileRepository, fileStatusRepository, disallowedPuidsRepository, uuidSource)
    val fileService = new FileService(fileRepository, fileStatusRepository, consignmentRepository, ffidMetadataService,
      antivirusMetadataService, fileStatusService, fileMetadataService, new CurrentTimeSource, uuidSource, config)
    val consignmentStatusService = new ConsignmentStatusService(consignmentStatusRepository, uuidSource, timeSource)
    val customMetadataPropertiesService = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)

    IO(
      ConsignmentApiContext(
        accessToken,
        antivirusMetadataService,
        clientFileMetadataService,
        consignmentService,
        ffidMetadataService,
        fileMetadataService,
        fileService,
        finalTransferConfirmationService,
        seriesService,
        transferAgreementService,
        transferringBodyService,
        consignmentStatusService,
        fileStatusService,
        customMetadataPropertiesService
      )
    )
  }
  //scalastyle:on method.length

  def executeGraphQLQuery(query: Document, operation: Option[String], vars: Json, accessToken: Token, database: JdbcBackend#DatabaseDef)
                                 (implicit ec: ExecutionContext): IO[Json] = {
    val context: IO[ConsignmentApiContext] = generateConsignmentApiContext(accessToken: Token, database)
    context.flatMap { ctx =>
      IO.fromFuture(IO(Executor.execute(
        GraphQlTypes.schema,
        query, ctx,
        variables = vars,
        operationName = operation,
        deferredResolver = new DeferredResolver,
        middleware = new ValidationAuthoriser :: new ConsignmentStateValidator :: Nil,
        exceptionHandler = exceptionHandler
      ))).recover {
        case error: QueryAnalysisError => error.resolveError
      }

      //        .map(OK -> _).recover {
      //        case error: QueryAnalysisError => BadRequest -> error.resolveError
      //        case error: ErrorWithResolver => InternalServerError -> error.resolveError
      //      }
    }
  }

}

object GraphQLServer {
  def apply(): GraphQLServer = new GraphQLServer()
}
