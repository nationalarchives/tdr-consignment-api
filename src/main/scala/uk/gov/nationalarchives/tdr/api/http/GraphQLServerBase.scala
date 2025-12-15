package uk.gov.nationalarchives.tdr.api.http

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import sangria.execution._
import sangria.marshalling.ResultMarshaller
import slick.jdbc.JdbcBackend
import sttp.client3.SimpleHttpClient
import uk.gov.nationalarchives.tdr.api.auth.AuthorisationException
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ConsignmentStateException
import uk.gov.nationalarchives.tdr.api.db.repository._
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.graphql.{ConsignmentApiContext, ErrorCodes}
import uk.gov.nationalarchives.tdr.api.service._
import uk.gov.nationalarchives.tdr.keycloak.Token

import scala.concurrent.ExecutionContext

trait GraphQLServerBase {

  private val logger = Logger(s"${getClass}")
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
    // Sangria catches all unhandled exceptions and returns a response. We'll rethrow them here so Pekko can deal with them.
    case (_, ex: Throwable) => throw ex
  }

  protected def generateConsignmentApiContext(accessToken: Token, db: JdbcBackend#Database)(implicit ec: ExecutionContext): ConsignmentApiContext = {
    val uuidSourceClass: Class[_] = Class.forName(config.getString("source.uuid"))
    val uuidSource: UUIDSource = uuidSourceClass.getDeclaredConstructor().newInstance().asInstanceOf[UUIDSource]
    val timeSource = new CurrentTimeSource
    val seriesRepository = new SeriesRepository(db)
    val consignmentRepository = new ConsignmentRepository(db, timeSource)
    val fileMetadataRepository = new FileMetadataRepository(db)
    val fileRepository = new FileRepository(db)
    val ffidMetadataRepository = new FFIDMetadataRepository(db)
    val ffidMetadataMatchesRepository = new FFIDMetadataMatchesRepository(db)
    val consignmentStatusRepository = new ConsignmentStatusRepository(db)
    val antivirusMetadataRepository = new AntivirusMetadataRepository(db)
    val fileStatusRepository = new FileStatusRepository(db)
    val metadataReviewLogRepository = new MetadataReviewLogRepository(db)
    val transferringBodyService = new TransferringBodyService(new TransferringBodyRepository(db))
    val consignmentService = new ConsignmentService(
      consignmentRepository,
      consignmentStatusRepository,
      seriesRepository,
      fileMetadataRepository,
      transferringBodyService,
      timeSource,
      uuidSource,
      config
    )
    val seriesService = new SeriesService(seriesRepository, uuidSource)
    val consignmentMetadataRepository = new ConsignmentMetadataRepository(db)
    val transferAgreementService = new TransferAgreementService(consignmentMetadataRepository, consignmentStatusRepository, uuidSource, timeSource)
    val finalTransferConfirmationService = new FinalTransferConfirmationService(new ConsignmentMetadataRepository(db), consignmentStatusRepository, uuidSource, timeSource)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepository, uuidSource, timeSource)
    val consignmentStatusService = new ConsignmentStatusService(consignmentStatusRepository, metadataReviewLogRepository, uuidSource, timeSource)
    val consignmentMetadataService = new ConsignmentMetadataService(consignmentMetadataRepository, uuidSource, timeSource)
    val fileStatusService = new FileStatusService(fileStatusRepository)
    val fileMetadataService =
      new FileMetadataService(fileMetadataRepository)
    val ffidMetadataService = new FFIDMetadataService(ffidMetadataRepository, ffidMetadataMatchesRepository, timeSource, uuidSource)
    val referenceGeneratorService = new ReferenceGeneratorService(config, SimpleHttpClient())
    val fileService = new FileService(
      fileRepository,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      referenceGeneratorService,
      new CurrentTimeSource,
      uuidSource,
      config
    )
    ConsignmentApiContext(
      accessToken,
      antivirusMetadataService,
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
      consignmentMetadataService
    )

  }
}
