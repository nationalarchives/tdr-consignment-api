package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.time.ZonedDateTime
import java.util.UUID
import akka.http.scaladsl.server.RequestContext
import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.relay._
import sangria.schema.{Argument, BooleanType, Field, InputObjectType, IntType, ListType, ObjectType, OptionInputType, OptionType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.auth._
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ValidateNoPreviousUploadForConsignment
import uk.gov.nationalarchives.tdr.api.db.repository.FileFilters
import uk.gov.nationalarchives.tdr.api.graphql._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{File, FileMetadataValues}

import scala.concurrent.ExecutionContext.Implicits.global

object ConsignmentFields {

  case class Consignment(consignmentid: UUID,
                         userid: UUID,
                         seriesid: Option[UUID],
                         createdDateTime: ZonedDateTime,
                         transferInitiatedDatetime: Option[ZonedDateTime],
                         exportDatetime: Option[ZonedDateTime],
                         exportLocation: Option[String],
                         consignmentReference: String,
                         consignmentType: String,
                         bodyId: UUID
                        )

  case class ConsignmentEdge(node: Consignment, cursor: String) extends Edge[Consignment]
  case class FileEdge(node: File, cursor: String) extends Edge[File]

  case class AddConsignmentInput(seriesid: Option[UUID] = None, consignmentType: String)

  case class AntivirusProgress(filesProcessed: Int)

  case class ChecksumProgress(filesProcessed: Int)

  case class FFIDProgress(filesProcessed: Int)

  case class FileChecks(antivirusProgress: AntivirusProgress, checksumProgress: ChecksumProgress, ffidProgress: FFIDProgress)
  case class TransferringBody(name: String, tdrCode: String)
  case class CurrentStatus(series: Option[String],
                           transferAgreement: Option[String],
                           upload: Option[String],
                           confirmTransfer: Option[String],
                           `export`: Option[String])
  case class StartUploadInput(consignmentId: UUID, parentFolder: String) extends UserOwnsConsignment

  case class UpdateExportDataInput(consignmentId: UUID, exportLocation: String, exportDatetime: Option[ZonedDateTime], exportVersion: String)

  case class UpdateConsignmentSeriesIdInput(consignmentId: UUID, seriesId: UUID) extends UserOwnsConsignment

  case class PaginationInput(limit: Int, currentCursor: Option[String])

  implicit val FileChecksType: ObjectType[Unit, FileChecks] =
    deriveObjectType[Unit, FileChecks]()
  implicit val AntivirusProgressType: ObjectType[Unit, AntivirusProgress] =
    deriveObjectType[Unit, AntivirusProgress]()
  implicit val ChecksumProgressType: ObjectType[Unit, ChecksumProgress] =
    deriveObjectType[Unit, ChecksumProgress]()
  implicit val FfidProgressType: ObjectType[Unit, FFIDProgress] =
    deriveObjectType[Unit, FFIDProgress]()
  implicit val TransferringBodyType: ObjectType[Unit, TransferringBody] =
    deriveObjectType[Unit, TransferringBody]()
  implicit val FileType: ObjectType[Unit, File] =
    deriveObjectType[Unit, File]()
  implicit val FileMetadataType: ObjectType[Unit, FileMetadataValues] = {
    deriveObjectType[Unit, FileMetadataValues]()
  }
  implicit val CurrentStatusType: ObjectType[Unit, CurrentStatus] =
    deriveObjectType[Unit, CurrentStatus]()

  implicit val PaginationInputType: InputObjectType[PaginationInput] = deriveInputObjectType[PaginationInput]()
  implicit val FileFiltersInputType: InputObjectType[FileFilters] = deriveInputObjectType[FileFilters]()
  val PaginationInputArg: Argument[Option[PaginationInput]] = Argument("paginationInput", OptionInputType(PaginationInputType))
  val FileFiltersInputArg: Argument[Option[FileFilters]] = Argument("fileFiltersInput", OptionInputType(FileFiltersInputType))

  implicit val ConnectionDefinition(_, fileConnections) =
    Connection.definition[RequestContext, Connection, File](
      name = "File",
      nodeType = FileType
    )

  implicit val ConsignmentType: ObjectType[Unit, Consignment] = ObjectType(
    "Consignment",
    fields[Unit, Consignment](
      Field("consignmentid", OptionType(UuidType), resolve = _.value.consignmentid),
      Field("userid", UuidType, resolve = _.value.userid),
      Field("seriesid", OptionType(UuidType), resolve = _.value.seriesid),
      Field("createdDatetime", OptionType(ZonedDateTimeType), resolve = _.value.createdDateTime),
      Field("transferInitiatedDatetime", OptionType(ZonedDateTimeType), resolve = _.value.transferInitiatedDatetime),
      Field("exportDatetime", OptionType(ZonedDateTimeType), resolve = _.value.exportDatetime),
      Field("exportLocation", OptionType(StringType), resolve = _.value.exportLocation),
      Field("consignmentType", OptionType(StringType), resolve = _.value.consignmentType),
      Field(
        "allChecksSucceeded",
        BooleanType,
        resolve = context => DeferChecksSucceeded(context.value.consignmentid)
      ),
      Field(
        "totalFiles",
        IntType,
        resolve = context => DeferTotalFiles(context.value.consignmentid)
      ),
      Field(
        "fileChecks",
        FileChecksType,
        resolve = context => DeferFileChecksProgress(context.value.consignmentid)
      ),
      Field(
        "parentFolder",
        OptionType(StringType),
        resolve = context => DeferParentFolder(context.value.consignmentid)
      ),
      Field(
        "parentFolderId",
        OptionType(UuidType),
        resolve = context => DeferParentFolderId(context.value.consignmentid)
      ),
      Field(
        "series",
        OptionType(SeriesFields.SeriesType),
        resolve = context => DeferConsignmentSeries(context.value.consignmentid)
      ),
      Field(
        "transferringBody",
        OptionType(TransferringBodyType),
        resolve = context => DeferConsignmentBody(context.value.consignmentid)
      ),
      Field(
        "files",
        ListType(FileType),
        arguments = FileFiltersInputArg :: Nil,
        resolve = ctx => {
          val fileFilters = ctx.args.argOpt("fileFiltersInput")
          DeferFiles(ctx.value.consignmentid, fileFilters)
        }
      ),
      Field(
        "paginatedFiles",
        fileConnections,
        arguments = PaginationInputArg :: Nil,
        resolve = ctx => {
          val paginationInput = ctx.args.argOpt("paginationInput")
          DeferPaginatedFiles(ctx.value.consignmentid, paginationInput)
        }
      ),
      Field(
        "consignmentReference",
        StringType,
        resolve = _.value.consignmentReference
      ),
      Field(
        "currentStatus",
        CurrentStatusType,
        resolve = context => DeferCurrentConsignmentStatus(context.value.consignmentid)
      )
    )
  )

  implicit val AddConsignmentInputType: InputObjectType[AddConsignmentInput] = deriveInputObjectType[AddConsignmentInput]()
  implicit val UpdateExportDataInputType: InputObjectType[UpdateExportDataInput] = deriveInputObjectType[UpdateExportDataInput]()
  implicit val StartUploadInputType: InputObjectType[StartUploadInput] = deriveInputObjectType[StartUploadInput]()
  implicit val ConnectionDefinition(_, consignmentConnections) =
    Connection.definition[RequestContext, Connection, Consignment](
      name = "Consignment",
      nodeType = ConsignmentType
    )
  implicit val UpdateConsignmentSeriesIdInputType: InputObjectType[UpdateConsignmentSeriesIdInput] = deriveInputObjectType[UpdateConsignmentSeriesIdInput]()

  val ConsignmentInputArg: Argument[AddConsignmentInput] = Argument("addConsignmentInput", AddConsignmentInputType)
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val ExportDataArg: Argument[UpdateExportDataInput] = Argument("exportData", UpdateExportDataInputType)
  val LimitArg: Argument[Int] = Argument("limit", IntType)
  val CurrentCursorArg: Argument[Option[String]] = Argument("currentCursor", OptionInputType(StringType))
  val StartUploadArg: Argument[StartUploadInput] = Argument("startUploadInput", StartUploadInputType)
  val UpdateConsignmentSeriesIdArg: Argument[UpdateConsignmentSeriesIdInput] =
    Argument("updateConsignmentSeriesId", UpdateConsignmentSeriesIdInputType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getConsignment", OptionType(ConsignmentType),
      arguments = ConsignmentIdArg :: PaginationInputArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.getConsignment(ctx.arg(ConsignmentIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field("consignments", consignmentConnections,
      arguments = List(LimitArg, CurrentCursorArg),
      resolve = ctx => {
        val limit: Int = ctx.args.arg("limit")
        val currentCursor = ctx.args.argOpt("currentCursor")
        ctx.ctx.consignmentService.getConsignments(limit, currentCursor)
          .map(r => {
            val endCursor = r.lastCursor
            val edges = r.consignmentEdges
            DefaultConnection(
              PageInfo(
                startCursor = edges.headOption.map(_.cursor),
                endCursor = endCursor,
                hasNextPage = endCursor.isDefined,
                hasPreviousPage = currentCursor.isDefined
              ),
              edges
            )
          }
        )
      },
      tags = List(ValidateHasReportingAccess)
    )
  )

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("addConsignment", ConsignmentType,
      arguments = ConsignmentInputArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.addConsignment(
        ctx.arg(ConsignmentInputArg),
        ctx.ctx.accessToken
      ),
      tags = Nil
    ),
    Field("updateTransferInitiated", OptionType(IntType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateTransferInitiated(ctx.arg(ConsignmentIdArg),
        ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field("updateExportData", OptionType(IntType),
      arguments = ExportDataArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateExportData(ctx.arg(ExportDataArg)),
      tags = List(ValidateHasExportAccess)
    ),
    Field("startUpload", StringType,
      arguments = StartUploadArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.startUpload(ctx.arg(StartUploadArg)),
      tags = List(ValidateUserHasAccessToConsignment(StartUploadArg), ValidateNoPreviousUploadForConsignment)
    ),
    Field("updateConsignmentSeriesId", OptionType(IntType),
      arguments = UpdateConsignmentSeriesIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateSeriesIdOfConsignment(ctx.arg(UpdateConsignmentSeriesIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(UpdateConsignmentSeriesIdArg), ValidateUpdateConsignmentSeriesId)
    )
  )
}
