package uk.gov.nationalarchives.tdr.api.graphql.fields

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.relay._
import sangria.schema.{
  Argument,
  BooleanType,
  EnumType,
  Field,
  InputObjectType,
  IntType,
  ListType,
  LongType,
  ObjectType,
  OptionInputType,
  OptionType,
  ProjectedName,
  Projector,
  StringType,
  fields
}
import uk.gov.nationalarchives.Tables.ConsignmentRow
import uk.gov.nationalarchives.tdr.api.auth._
import uk.gov.nationalarchives.tdr.api.consignmentstatevalidation.ValidateNoPreviousUploadForConsignment
import uk.gov.nationalarchives.tdr.api.db.repository.{FileFilters, FileMetadataFilters}
import uk.gov.nationalarchives.tdr.api.graphql._
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentStatusFields.ConsignmentStatus
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import uk.gov.nationalarchives.tdr.api.graphql.validation.UserOwnsConsignment
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{File, FileMetadataValue, FileMetadataValues}
import uk.gov.nationalarchives.tdr.api.service.FileService.TDRConnection

import java.time.ZonedDateTime
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

object ConsignmentFields {

  case class Consignment(
      consignmentid: UUID,
      userid: UUID,
      seriesid: Option[UUID],
      createdDateTime: ZonedDateTime,
      transferInitiatedDatetime: Option[ZonedDateTime],
      exportDatetime: Option[ZonedDateTime],
      exportLocation: Option[String],
      consignmentReference: String,
      consignmentType: String,
      bodyId: UUID,
      includeTopLevelFolder: Option[Boolean],
      seriesName: Option[String],
      transferringBodyName: Option[String],
      transferringBodyTdrCode: Option[String],
      metadataSchemaLibraryVersion: Option[String],
      clientSideDraftMetadataFileName: Option[String]
  )

  case class ConsignmentEdge(node: Consignment, cursor: String) extends Edge[Consignment]

  case class FileEdge(node: File, cursor: String) extends Edge[File]

  case class AddConsignmentInput(seriesid: Option[UUID] = None, consignmentType: String)

  case class AntivirusProgress(filesProcessed: Int)

  case class ChecksumProgress(filesProcessed: Int)

  case class FFIDProgress(filesProcessed: Int)

  case class FileChecks(antivirusProgress: AntivirusProgress, checksumProgress: ChecksumProgress, ffidProgress: FFIDProgress)

  case class StartUploadInput(consignmentId: UUID, parentFolder: String, includeTopLevelFolder: Boolean = false) extends UserOwnsConsignment

  case class UpdateExportDataInput(consignmentId: UUID, exportLocation: String, exportDatetime: Option[ZonedDateTime], exportVersion: String)

  case class UpdateConsignmentSeriesIdInput(consignmentId: UUID, seriesId: UUID) extends UserOwnsConsignment

  case class UpdateMetadataSchemaLibraryVersionInput(consignmentId: UUID, metadataSchemaLibraryVersion: String) extends UserOwnsConsignment

  case class UpdateClientSideDraftMetadataFileNameInput(consignmentId: UUID, clientSideDraftMetadataFileName: String) extends UserOwnsConsignment

  case class PaginationInput(limit: Option[Int], currentPage: Option[Int], currentCursor: Option[String], fileFilters: Option[FileFilters])

  case class ConsignmentFilters(userId: Option[UUID], consignmentType: Option[String])

  case class ConsignmentOrderBy(consignmentOrderField: ConsignmentOrderField, orderDirection: Direction)

  sealed trait ConsignmentOrderField {
    val cursorFn: ConsignmentRow => String
  }

  case object ConsignmentReference extends ConsignmentOrderField {
    override val cursorFn: ConsignmentRow => String = _.consignmentreference
  }
  case object CreatedAtTimestamp extends ConsignmentOrderField {
    override val cursorFn: ConsignmentRow => String = _.datetime.toString
  }

  sealed trait Direction
  case object Ascending extends Direction
  case object Descending extends Direction

  implicit val consignmentOrderFieldDecoder: Decoder[ConsignmentOrderField] = Decoder[String].emap {
    case "ConsignmentReference" => Right(ConsignmentReference)
    case "CreatedAtTimestamp"   => Right(CreatedAtTimestamp)
    case other                  => Left(s"Unknown ConsignmentOrderField: $other")
  }

  implicit val consignmentOrderFieldEncoder: Encoder[ConsignmentOrderField] = Encoder[String].contramap {
    case ConsignmentReference => "ConsignmentReference"
    case CreatedAtTimestamp   => "CreatedAtTimestamp"
  }

  implicit val directionDecoder: Decoder[Direction] = Decoder[String].emap {
    case "Ascending"  => Right(Ascending)
    case "Descending" => Right(Descending)
    case other        => Left(s"Unknown Direction: $other")
  }

  implicit val directionEncoder: Encoder[Direction] = Encoder[String].contramap {
    case Ascending  => "Ascending"
    case Descending => "Descending"
  }

  implicit val FileChecksType: ObjectType[Unit, FileChecks] =
    deriveObjectType[Unit, FileChecks]()
  implicit val AntivirusProgressType: ObjectType[Unit, AntivirusProgress] =
    deriveObjectType[Unit, AntivirusProgress]()
  implicit val ChecksumProgressType: ObjectType[Unit, ChecksumProgress] =
    deriveObjectType[Unit, ChecksumProgress]()
  implicit val FfidProgressType: ObjectType[Unit, FFIDProgress] =
    deriveObjectType[Unit, FFIDProgress]()
  implicit val FileMetadataValueType: ObjectType[Unit, FileMetadataValue] =
    deriveObjectType[Unit, FileMetadataValue]()
  implicit val FileType: ObjectType[Unit, File] =
    deriveObjectType[Unit, File]()
  implicit val FileMetadataType: ObjectType[Unit, FileMetadataValues] = {
    deriveObjectType[Unit, FileMetadataValues]()
  }
  implicit val ConsignmentStatusType: ObjectType[Unit, ConsignmentStatus] =
    deriveObjectType[Unit, ConsignmentStatus]()

  implicit val ConsignmentOrderFieldType: EnumType[ConsignmentOrderField] = deriveEnumType[ConsignmentOrderField]()
  implicit val DirectionType: EnumType[Direction] = deriveEnumType[Direction]()

  implicit val PaginationInputType: InputObjectType[PaginationInput] = deriveInputObjectType[PaginationInput]()
  implicit val FileMetadataFiltersInputType: InputObjectType[FileMetadataFilters] = deriveInputObjectType[FileMetadataFilters]()
  implicit val FileFiltersInputType: InputObjectType[FileFilters] = deriveInputObjectType[FileFilters]()
  implicit val ConsignmentFiltersInputType: InputObjectType[ConsignmentFilters] = deriveInputObjectType[ConsignmentFilters]()
  implicit val ConsignmentOrderByType: InputObjectType[ConsignmentOrderBy] = deriveInputObjectType[ConsignmentOrderBy]()

  val PaginationInputArg: Argument[Option[PaginationInput]] = Argument("paginationInput", OptionInputType(PaginationInputType))
  val FileFiltersInputArg: Argument[Option[FileFilters]] = Argument("fileFiltersInput", OptionInputType(FileFiltersInputType))
  val ConsignmentFiltersInputArg: Argument[Option[ConsignmentFilters]] = Argument("consignmentFiltersInput", OptionInputType(ConsignmentFiltersInputType))

  def getQueriedFileFields(projected: Vector[ProjectedName]): QueriedFileFields = QueriedFileFields(
    projected.exists(_.name == "originalFilePath"),
    projected.exists(_.name == "antivirusMetadata"),
    projected.exists(_.name == "ffidMetadata"),
    projected.exists(_.name == "fileStatus"),
    projected.exists(_.name == "fileStatuses")
  )

  implicit val ConnectionDefinition(_, fileConnections) =
    Connection.definition[ConsignmentApiContext, TDRConnection, File](
      name = "File",
      nodeType = FileType,
      connectionFields = fields[ConsignmentApiContext, TDRConnection[File]](
        Field(
          "totalPages",
          OptionType(IntType),
          resolve = ctx => ctx.value.totalPages
        ),
        Field(
          "totalItems",
          OptionType(IntType),
          resolve = ctx => ctx.value.totalItems
        )
      )
    )

  implicit val ConsignmentType: ObjectType[ConsignmentApiContext, Consignment] = ObjectType(
    "Consignment",
    fields[ConsignmentApiContext, Consignment](
      Field("consignmentid", OptionType(UuidType), resolve = _.value.consignmentid),
      Field("userid", UuidType, resolve = _.value.userid),
      Field("seriesid", OptionType(UuidType), resolve = _.value.seriesid),
      Field("createdDatetime", OptionType(ZonedDateTimeType), resolve = _.value.createdDateTime),
      Field("transferInitiatedDatetime", OptionType(ZonedDateTimeType), resolve = _.value.transferInitiatedDatetime),
      Field("exportDatetime", OptionType(ZonedDateTimeType), resolve = _.value.exportDatetime),
      Field("exportLocation", OptionType(StringType), resolve = _.value.exportLocation),
      Field("consignmentType", OptionType(StringType), resolve = _.value.consignmentType),
      Field("includeTopLevelFolder", OptionType(BooleanType), resolve = _.value.includeTopLevelFolder),
      Field("seriesName", OptionType(StringType), resolve = _.value.seriesName),
      Field("transferringBodyName", OptionType(StringType), resolve = _.value.transferringBodyName),
      Field("transferringBodyTdrCode", OptionType(StringType), resolve = _.value.transferringBodyTdrCode),
      Field("metadataSchemaLibraryVersion", OptionType(StringType), resolve = _.value.metadataSchemaLibraryVersion),
      Field("clientSideDraftMetadataFileName", OptionType(StringType), resolve = _.value.clientSideDraftMetadataFileName),
      Field(
        "allChecksSucceeded",
        BooleanType,
        resolve = context => DeferChecksSucceeded(context.value.consignmentid)
      ),
      Field(
        "totalClosedRecords",
        IntType,
        resolve = context => DeferClosedRecords(context.value.consignmentid)
      ),
      Field(
        "totalFiles",
        IntType,
        resolve = context => DeferTotalFiles(context.value.consignmentid)
      ),
      Field(
        "totalFileSize",
        LongType,
        resolve = context => DeferFileSizeSum(context.value.consignmentid)
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
        "files",
        ListType(FileType),
        arguments = FileFiltersInputArg :: Nil,
        resolve = Projector((ctx, projected) => {
          val fileFilters = ctx.args.argOpt("fileFiltersInput")
          val queriedFileFields = getQueriedFileFields(projected)
          DeferFiles(ctx.value.consignmentid, fileFilters, queriedFileFields)
        })
      ),
      Field(
        "paginatedFiles",
        fileConnections,
        arguments = PaginationInputArg :: Nil,
        resolve = Projector((ctx, projected) => {
          val paginationInput = ctx.args.argOpt("paginationInput")
          val queriedFileFields = getQueriedFileFields(projected)
          DeferPaginatedFiles(ctx.value.consignmentid, paginationInput, queriedFileFields)
        })
      ),
      Field(
        "consignmentReference",
        StringType,
        resolve = _.value.consignmentReference
      ),
      Field(
        "consignmentStatuses",
        ListType(ConsignmentStatusType),
        resolve = context => DeferConsignmentStatuses(context.value.consignmentid)
      )
    )
  )

  implicit val AddConsignmentInputType: InputObjectType[AddConsignmentInput] = deriveInputObjectType[AddConsignmentInput]()
  implicit val UpdateExportDataInputType: InputObjectType[UpdateExportDataInput] = deriveInputObjectType[UpdateExportDataInput]()
  implicit val StartUploadInputType: InputObjectType[StartUploadInput] = deriveInputObjectType[StartUploadInput]()
  implicit val UpdateConsignmentSeriesIdInputType: InputObjectType[UpdateConsignmentSeriesIdInput] = deriveInputObjectType[UpdateConsignmentSeriesIdInput]()
  implicit val UpdateMetadataSchemaLibraryVersionInputType: InputObjectType[UpdateMetadataSchemaLibraryVersionInput] =
    deriveInputObjectType[UpdateMetadataSchemaLibraryVersionInput]()
  implicit val UpdateClientSideDraftMetadataFileNameType: InputObjectType[UpdateClientSideDraftMetadataFileNameInput] =
    deriveInputObjectType[UpdateClientSideDraftMetadataFileNameInput]()

  val ConsignmentInputArg: Argument[AddConsignmentInput] = Argument("addConsignmentInput", AddConsignmentInputType)
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val ExportDataArg: Argument[UpdateExportDataInput] = Argument("exportData", UpdateExportDataInputType)
  val LimitArg: Argument[Int] = Argument("limit", IntType)
  val CurrentCursorArg: Argument[Option[String]] = Argument("currentCursor", OptionInputType(StringType))
  val CurrentPageArg: Argument[Option[Int]] = Argument("currentPage", OptionInputType(IntType))
  val ConsignmentOrderByArg: Argument[Option[ConsignmentOrderBy]] = Argument("orderBy", OptionInputType(ConsignmentOrderByType))
  val StartUploadArg: Argument[StartUploadInput] = Argument("startUploadInput", StartUploadInputType)
  val UpdateConsignmentSeriesIdArg: Argument[UpdateConsignmentSeriesIdInput] =
    Argument("updateConsignmentSeriesId", UpdateConsignmentSeriesIdInputType)
  val UpdateMetadataSchemaLibraryVersionArg: Argument[UpdateMetadataSchemaLibraryVersionInput] =
    Argument("updateMetadataSchemaLibraryVersion", UpdateMetadataSchemaLibraryVersionInputType)
  val UpdateClientSideDraftMetadataFileNameInputArg: Argument[UpdateClientSideDraftMetadataFileNameInput] =
    Argument("updateClientSideDraftMetadataFileName", UpdateClientSideDraftMetadataFileNameType)

  implicit val ConnectionDefinition(_, consignmentConnections) =
    Connection.definition[ConsignmentApiContext, Connection, Consignment](
      name = "Consignment",
      nodeType = ConsignmentType,
      connectionFields = fields[ConsignmentApiContext, Connection[Consignment]](
        Field(
          "totalPages",
          OptionType(IntType),
          arguments = LimitArg :: ConsignmentFiltersInputArg :: Nil,
          resolve = ctx => ctx.ctx.consignmentService.getTotalPages(ctx.arg(LimitArg), ctx.arg(ConsignmentFiltersInputArg))
        )
      )
    )

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "getConsignment",
      OptionType(ConsignmentType),
      arguments = ConsignmentIdArg :: PaginationInputArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.getConsignment(ctx.arg(ConsignmentIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field(
      "consignments",
      consignmentConnections,
      arguments = List(LimitArg, CurrentCursorArg, CurrentPageArg, ConsignmentFiltersInputArg, ConsignmentOrderByArg),
      resolve = ctx => {
        val limit: Int = ctx.args.arg("limit")
        val currentCursor = ctx.args.argOpt("currentCursor")
        val currentPage = ctx.args.argOpt("currentPage")
        val consignmentFilters = ctx.args.argOpt("consignmentFiltersInput")
        val consignmentOrderBy = ctx.args.argOpt("orderBy")
        ctx.ctx.consignmentService
          .getConsignments(limit, currentCursor, consignmentFilters, currentPage, consignmentOrderBy)
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
          })
      },
      tags = List(ValidateHasConsignmentsAccess)
    ),
    Field(
      "getConsignmentsForMetadataReview",
      ListType(ConsignmentType),
      arguments = Nil,
      resolve = ctx => ctx.ctx.consignmentService.getConsignmentsForMetadataReview,
      tags = List(ValidateIsTnaUser)
    )
  )

  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field(
      "addConsignment",
      ConsignmentType,
      arguments = ConsignmentInputArg :: Nil,
      resolve = ctx =>
        ctx.ctx.consignmentService.addConsignment(
          ctx.arg(ConsignmentInputArg),
          ctx.ctx.accessToken
        ),
      tags = Nil
    ),
    Field(
      "updateTransferInitiated",
      OptionType(IntType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateTransferInitiated(ctx.arg(ConsignmentIdArg), ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field(
      "updateExportData",
      OptionType(IntType),
      arguments = ExportDataArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateExportData(ctx.arg(ExportDataArg)),
      tags = List(ValidateHasExportAccess)
    ),
    Field(
      "startUpload",
      StringType,
      arguments = StartUploadArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.startUpload(ctx.arg(StartUploadArg)),
      tags = List(ValidateUserHasAccessToConsignment(StartUploadArg), ValidateNoPreviousUploadForConsignment)
    ),
    Field(
      "updateConsignmentSeriesId",
      OptionType(IntType),
      arguments = UpdateConsignmentSeriesIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateSeriesOfConsignment(ctx.arg(UpdateConsignmentSeriesIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(UpdateConsignmentSeriesIdArg), ValidateUpdateConsignmentSeriesId)
    ),
    Field(
      "updateConsignmentMetadataSchemaLibraryVersion",
      OptionType(IntType),
      arguments = UpdateMetadataSchemaLibraryVersionArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateMetadataSchemaLibraryVersion(ctx.arg(UpdateMetadataSchemaLibraryVersionArg)),
      tags = List(ValidateUserHasAccessToConsignment(UpdateMetadataSchemaLibraryVersionArg))
    ),
    Field(
      "updateClientSideDraftMetadataFileName",
      OptionType(IntType),
      arguments = UpdateClientSideDraftMetadataFileNameInputArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateClientSideDraftMetadataFileName(ctx.arg(UpdateClientSideDraftMetadataFileNameInputArg)),
      tags = List(ValidateUserHasAccessToConsignment(UpdateClientSideDraftMetadataFileNameInputArg))
    )
  )
}
