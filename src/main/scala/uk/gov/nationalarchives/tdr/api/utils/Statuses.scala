package uk.gov.nationalarchives.tdr.api.utils

// Legacy status types not yet in tdr-common-utils.
// These may have been removed with the transition to the CSV metadata upload route.
// TODO: Investigate whether ClosureMetadataType and DescriptiveMetadataType are still used;
//  if not, remove this file. If still needed, add them to common-utils instead.
object Statuses {
  case object ClosureMetadataType { val id: String = "ClosureMetadata" }
  case object DescriptiveMetadataType { val id: String = "DescriptiveMetadata" }
}
