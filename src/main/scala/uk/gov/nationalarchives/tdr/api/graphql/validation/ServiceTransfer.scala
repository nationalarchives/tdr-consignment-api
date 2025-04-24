package uk.gov.nationalarchives.tdr.api.graphql.validation

import java.util.UUID

trait ServiceTransfer {
  def userIdOverride: Option[UUID]
}
