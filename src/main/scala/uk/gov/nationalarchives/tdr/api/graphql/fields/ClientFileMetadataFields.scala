package uk.gov.nationalarchives.tdr.api.graphql.fields

import java.util.UUID

import io.circe.generic.auto._
import sangria.macros.derive._
import sangria.marshalling.circe._
import sangria.schema.{Argument, Field, ObjectType, fields}
import uk.gov.nationalarchives.tdr.api.auth.ValidateHasClientFileMetadataAccess
import uk.gov.nationalarchives.tdr.api.graphql.ConsignmentApiContext
import uk.gov.nationalarchives.tdr.api.graphql.fields.FieldTypes._

object ClientFileMetadataFields {
  case class ClientFileMetadata(fileId: UUID,
                                originalPath: Option[String] = None,
                                checksum: Option[String] = None,
                                checksumType: Option[String] = None,
                                lastModified: Long,
                                fileSize: Option[Long] = None)

  implicit val ClientFileMetadataType: ObjectType[Unit, ClientFileMetadata] = deriveObjectType[Unit, ClientFileMetadata]()

  val FileIdArg = Argument("fileId", UuidType)

  val queryFields: List[Field[ConsignmentApiContext, Unit]] = fields[ConsignmentApiContext, Unit](
    Field("getClientFileMetadata", ClientFileMetadataType,
      arguments=FileIdArg :: Nil,
      resolve = ctx => ctx.ctx.clientFileMetadataService.getClientFileMetadata(ctx.arg(FileIdArg)),
      tags=ValidateHasClientFileMetadataAccess :: Nil
      //We're only using this for the file metadata api update lambda for now. This check can be changed if we use it anywhere else
    ))
}
