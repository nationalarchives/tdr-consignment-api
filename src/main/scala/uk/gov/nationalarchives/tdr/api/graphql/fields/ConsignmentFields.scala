package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.time.ZonedDateTime
import java.util.UUID

import akka.http.scaladsl.server.RequestContext
import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.relay.util.Base64
import sangria.relay.{Connection, ConnectionDefinition, DefaultConnection, Edge, PageInfo}
import sangria.schema.{Argument, BooleanType, Field, InputObjectType, IntType, ListType, ObjectType, OptionType, StringType, fields}
import uk.gov.nationalarchives.tdr.api.auth.{ValidateHasExportAccess, ValidateHasReportingAccess, ValidateSeries, ValidateUserHasAccessToConsignment}
import uk.gov.nationalarchives.tdr.api.graphql._
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{File, FileMetadataValues}

import scala.concurrent.ExecutionContext.Implicits.global

object ConsignmentFields {
  case class PaginatedResult[T](totalCount: Int, entities: List[T])
  case class Consignment(consignmentid: UUID,
                         userid: UUID,
                         seriesid: UUID,
                         createdDateTime: ZonedDateTime,
                         transferInitiatedDatetime: Option[ZonedDateTime],
                         exportDatetime: Option[ZonedDateTime],
                         consignmentReference: String
                        )

  case class ConsignmentEdge(node: Consignment, cursor: String) extends Edge[Consignment]

  case class AddConsignmentInput(seriesid: UUID)

  case class AntivirusProgress(filesProcessed: Int)

  case class ChecksumProgress(filesProcessed: Int)

  case class FFIDProgress(filesProcessed: Int)

  case class FileChecks(antivirusProgress: AntivirusProgress, checksumProgress: ChecksumProgress, ffidProgress: FFIDProgress)
  case class TransferringBody(name: String, tdrCode: String)
  case class CurrentStatus(upload: Option[String])

  case class UpdateExportLocationInput(consignmentId: UUID, exportLocation: String, exportDatetime: Option[ZonedDateTime])

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



  private val LimitArg = Argument("limit", IntType)
  private val CurrentCursorArg = Argument("currentCursor", StringType)
  private val PageNumberArg = Argument("pageNumber", IntType)

  private def decode(value: String) = {
    Base64.decode(value) match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("Invalid cursor: " + value)
    }
  }

  implicit val ConsignmentType: ObjectType[Unit, Consignment] = ObjectType(
    "Consignment",
    fields[Unit, Consignment](
      Field("consignmentid", OptionType(UuidType), resolve = _.value.consignmentid),
      Field("userid", UuidType, resolve = _.value.userid),
      Field("seriesid", UuidType, resolve = _.value.seriesid),
      Field("createdDatetime", OptionType(ZonedDateTimeType), resolve = _.value.createdDateTime),
      Field("transferInitiatedDatetime", OptionType(ZonedDateTimeType), resolve = _.value.transferInitiatedDatetime),
      Field("exportDatetime", OptionType(ZonedDateTimeType), resolve = _.value.exportDatetime),
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
        resolve = context => DeferFiles(context.value.consignmentid)
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

  implicit private val ConnectionDefinition(_, consignmentConnections) =
    Connection.definition[RequestContext, Connection, Consignment](
      name = "Consignment",
      nodeType = ConsignmentType
    )

  implicit val AddConsignmentInputType: InputObjectType[AddConsignmentInput] = deriveInputObjectType[AddConsignmentInput]()
  implicit val UpdateExportLocationInputType: InputObjectType[UpdateExportLocationInput] = deriveInputObjectType[UpdateExportLocationInput]()

  val ConsignmentInputArg: Argument[AddConsignmentInput] = Argument("addConsignmentInput", AddConsignmentInputType)
  val ConsignmentIdArg: Argument[UUID] = Argument("consignmentid", UuidType)
  val ExportLocationArg: Argument[UpdateExportLocationInput] = Argument("exportLocation", UpdateExportLocationInputType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getConsignment", OptionType(ConsignmentType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.getConsignment(ctx.arg(ConsignmentIdArg)),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field("consignments", ListType(ConsignmentType),
      arguments = Nil,
      resolve = ctx => ctx.ctx.consignmentService.getConsignments(),
      tags = List(ValidateHasReportingAccess)
    ),
    Field("consignmentsPaginated", consignmentConnections,
      arguments = List(LimitArg, CurrentCursorArg),
      resolve = ctx => {
        val limit: Int = ctx.args.arg("limit")
        val currentCursor: String = ctx.args.arg("currentCursor")
        //val decodedCurrentCursor: String = decode(currentCursor)
        ctx.ctx.consignmentService.getConsignmentsPaginated(limit, currentCursor)
          .map(r => {
            val nextCursor = r._1
            val edges = r._2
            val firstEdge = edges.headOption
            DefaultConnection(
              PageInfo(
                startCursor = firstEdge.map(_.cursor),
                endCursor = Option(nextCursor),
                hasNextPage = !nextCursor.isEmpty,
                hasPreviousPage = !currentCursor.isEmpty
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
        ctx.ctx.accessToken.userId
      ),
      tags = List(ValidateSeries)
    ),
    Field("updateTransferInitiated", OptionType(IntType),
      arguments = ConsignmentIdArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateTransferInitiated(ctx.arg(ConsignmentIdArg),
        ctx.ctx.accessToken.userId),
      tags = List(ValidateUserHasAccessToConsignment(ConsignmentIdArg))
    ),
    Field("updateExportLocation", OptionType(IntType),
      arguments = ExportLocationArg :: Nil,
      resolve = ctx => ctx.ctx.consignmentService.updateExportLocation(ctx.arg(ExportLocationArg)),
      tags = List(ValidateHasExportAccess)
    )
  )
}
