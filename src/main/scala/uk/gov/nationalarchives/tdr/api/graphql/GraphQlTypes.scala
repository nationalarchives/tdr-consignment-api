package uk.gov.nationalarchives.tdr.api.graphql

import sangria.schema.{ObjectType, Schema}
import uk.gov.nationalarchives.tdr.api.graphql.fields._

object GraphQlTypes {

  private val QueryType = ObjectType(
    "Query",
    SeriesFields.queryFields ++
      ConsignmentFields.queryFields ++
      FileFields.queryFields
  )
  private val MutationType = ObjectType(
    "Mutation",
    ConsignmentFields.mutationFields ++
      ConsignmentMetadataFields.mutationFields ++
      TransferAgreementFields.mutationFields ++
      FileFields.mutationFields ++
      ConsignmentStatusFields.mutationFields ++
      AntivirusMetadataFields.mutationFields ++
      FileMetadataFields.mutationFields ++
      FFIDMetadataFields.mutationFields ++
      FinalTransferConfirmationFields.mutationFields ++
      FileStatusFields.mutationFields
  )

  val schema: Schema[ConsignmentApiContext, Unit] = Schema(QueryType, Some(MutationType))
}
