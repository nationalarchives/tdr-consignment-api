package uk.gov.nationalarchives.tdr.api.graphql.validation

import java.util.UUID

trait UserOwnsConsignment {
  def consignmentId: UUID
}
