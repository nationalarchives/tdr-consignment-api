package uk.gov.nationalarchives.tdr.api.http

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import sangria.execution._
import sangria.marshalling.ResultMarshaller
import slick.jdbc.JdbcBackend
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
    // Sangria catches all unhandled exceptions and returns a response. We'll rethrow them here so Akka can deal with them.
    case (_, ex: Throwable) => throw ex
  }

  protected def generateConsignmentApiContext(accessToken: Token, db: JdbcBackend#DatabaseDef)(implicit ec: ExecutionContext): ConsignmentApiContext = {
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
    val fileStatusRepository = new FileStatusRepository(db)
    val displayPropertiesRepository = new DisplayPropertiesRepository(db)
    val transferringBodyService = new TransferringBodyService(new TransferringBodyRepository(db))
    val consignmentService = new ConsignmentService(
      consignmentRepository,
      consignmentStatusRepository,
      transferringBodyService,
      timeSource,
      uuidSource,
      config
    )
    val seriesService = new SeriesService(new SeriesRepository(db), uuidSource)
    val transferAgreementService = new TransferAgreementService(new ConsignmentMetadataRepository(db), new ConsignmentStatusRepository(db), uuidSource, timeSource)
    val finalTransferConfirmationService = new FinalTransferConfirmationService(new ConsignmentMetadataRepository(db), consignmentStatusRepository, uuidSource, timeSource)
    val clientFileMetadataService = new ClientFileMetadataService(fileMetadataRepository)
    val antivirusMetadataService = new AntivirusMetadataService(antivirusMetadataRepository, uuidSource, timeSource)
    val customMetadataPropertiesRepository = new CustomMetadataPropertiesRepository(db)
    val customMetadataPropertiesService = new CustomMetadataPropertiesService(customMetadataPropertiesRepository)
    val validateFileMetadataService = new ValidateFileMetadataService(customMetadataPropertiesService, fileMetadataRepository, fileStatusRepository, timeSource, uuidSource)
    val consignmentStatusService = new ConsignmentStatusService(consignmentStatusRepository, fileStatusRepository, uuidSource, timeSource)
    val fileMetadataService = new FileMetadataService(
      fileMetadataRepository,
      fileRepository,
      consignmentStatusService,
      customMetadataPropertiesService,
      validateFileMetadataService,
      timeSource,
      uuidSource
    )
    val ffidMetadataService =
      new FFIDMetadataService(ffidMetadataRepository, ffidMetadataMatchesRepository, timeSource, uuidSource)
    val fileStatusService = new FileStatusService(fileStatusRepository, uuidSource)
    val displayPropertiesService = new DisplayPropertiesService(displayPropertiesRepository)
    val fileService = new FileService(
      fileRepository,
      consignmentRepository,
      customMetadataPropertiesRepository,
      ffidMetadataService,
      antivirusMetadataService,
      fileStatusService,
      fileMetadataService,
      new CurrentTimeSource,
      uuidSource,
      config
    )
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
      customMetadataPropertiesService,
      displayPropertiesService
    )

  }
}
