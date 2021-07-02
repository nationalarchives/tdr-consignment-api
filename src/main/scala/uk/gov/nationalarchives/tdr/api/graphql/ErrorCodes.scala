package uk.gov.nationalarchives.tdr.api.graphql

/**
  * GraphQL supports arbitrary error codes to provide extra error information to client applications. These error codes
  * are added to the error object under `extensions.code`.
  *
  * See the GraphQL spec for more information: https://github.com/graphql/graphql-spec/blob/master/spec/Section%207%20--%20Response.md#errors
  */
object ErrorCodes {
  val notAuthorised = "NOT_AUTHORISED"
  val invalidConsignmentState = "INVALID_CONSIGNMENT_STATE"
  val invalidInputData = "INVALID_INPUT_DATA"
}
