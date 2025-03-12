package uk.gov.nationalarchives.tdr.api.graphql.fields

import sangria.macros.derive.deriveInputObjectType
import io.circe.generic.auto._

import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, InputObjectType, ListType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateUserHasAccessToConsignment
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileFields.{AddFileAndMetadataInput, AddFileAndMetadataInputType, FileAndMetadataInputArg, FileSequenceType}

import java.util.UUID

object LoadDataCSVFields {
//  case class LoadDataCsvInput(consignmentId: UUID, userId: UUID, destinationTable: String, s3SourceUri: String)
//
//  implicit val LoadDataCsvInputType: InputObjectType[LoadDataCsvInput] = deriveInputObjectType[LoadDataCsvInput]()
//  private val LoadDataCsvInputArg = Argument("loadDataCsvInput", LoadDataCsvInputType)
//
//  val mutationFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
////    Field(
////      "loadCsvData",
////      ListType(FileSequenceType),
////      arguments = List(LoadDataCsvInputArg),
////      resolve = ctx => ctx.ctx.fileService.addFile(ctx.arg(FileAndMetadataInputArg), ctx.ctx.accessToken.userId),
////      tags = List(ValidateUserHasAccessToConsignment(FileAndMetadataInputArg))
////    )
//  )

}
