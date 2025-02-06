package uk.gov.nationalarchives.tdr.api.utils

import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues, Defined, Supplied, Text}

object TestDataHelper {

  def mockCustomMetadataFields(): Seq[CustomMetadataField] = {
    val closurePeriodField: CustomMetadataField =
      CustomMetadataField(
        "ClosurePeriod",
        Some("Closure Period"),
        None,
        Defined,
        Some("MandatoryClosure"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )
    val descriptionField: CustomMetadataField =
      CustomMetadataField(
        "description",
        Some("description"),
        None,
        Defined,
        Some("OptionalMetadata"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )
    val languageField =
      CustomMetadataField(
        "Language",
        Some("Language"),
        None,
        Defined,
        Some("OptionalMetadata"),
        Text,
        true,
        true,
        Some("English"),
        List(),
        2147483647,
        false,
        None
      )
    val alternativeDescriptionField: CustomMetadataField =
      CustomMetadataField(
        "AlternativeDescription",
        Some("Alternative Description"),
        None,
        Defined,
        Some("OptionalClosure"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )
    val alternativeTitleField: CustomMetadataField =
      CustomMetadataField(
        "TitleAlternate",
        Some("Alternative Title"),
        None,
        Defined,
        Some("OptionalClosure"),
        Text,
        true,
        false,
        None,
        List(),
        2147483647,
        false,
        None
      )
    val foiExemptionCodeField =
      CustomMetadataField(
        "FoiExemptionCode",
        Some("FOI Exemption Code"),
        None,
        Defined,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        None,
        List(),
        2147483647,
        false,
        None
      )

    val descriptionClosedTrueValues: CustomMetadataValues = CustomMetadataValues(List(alternativeDescriptionField), "true", 2147483647)
    val descriptionClosedFalseValues: CustomMetadataValues = CustomMetadataValues(List(), "false", 2147483647)
    val descriptionClosedField: CustomMetadataField =
      CustomMetadataField(
        "DescriptionClosed",
        Some("DescriptionClosed"),
        None,
        Supplied,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        Some("false"),
        List(descriptionClosedTrueValues, descriptionClosedFalseValues),
        2147483647,
        false,
        None
      )

    val titleClosedTrueValue: CustomMetadataValues = CustomMetadataValues(List(alternativeTitleField), "true", 2147483647)
    val titleClosedFalseValue: CustomMetadataValues = CustomMetadataValues(List(), "false", 2147483647)
    val titleClosedField: CustomMetadataField =
      CustomMetadataField(
        "TitleClosed",
        Some("TitleClosed"),
        None,
        Supplied,
        Some("MandatoryClosure"),
        Text,
        true,
        true,
        Some("false"),
        List(titleClosedTrueValue, titleClosedFalseValue),
        2147483647,
        false,
        None
      )

    val closureTypeClosedValues: CustomMetadataValues =
      CustomMetadataValues(List(closurePeriodField, foiExemptionCodeField, descriptionClosedField, titleClosedField), "Closed", 2147483647)
    val closureTypeOpenValues: CustomMetadataValues = CustomMetadataValues(List(), "Open", 2147483647)
    val closureTypeField: CustomMetadataField =
      CustomMetadataField(
        "ClosureType",
        Some("Closure Type"),
        None,
        Defined,
        Some("MandatoryClosure"),
        Text,
        true,
        false,
        Some("Open"),
        List(closureTypeClosedValues, closureTypeOpenValues),
        2147483647,
        false,
        None
      )

    val multiValueDependency40Value: CustomMetadataValues = CustomMetadataValues(List(closurePeriodField), "40", 2147483647)
    val multiValueDependency30Value: CustomMetadataValues = CustomMetadataValues(List(closurePeriodField), "30", 2147483647)
    val multiValueWithDependenciesField =
      CustomMetadataField(
        "MultiValueWithDependencies",
        Some("FOI Exemption Code"),
        None,
        Defined,
        Some("Group"),
        Text,
        true,
        true,
        None,
        List(multiValueDependency30Value, multiValueDependency40Value),
        2147483647,
        false,
        None
      )

    Seq(
      closurePeriodField,
      closureTypeField,
      descriptionField,
      alternativeDescriptionField,
      foiExemptionCodeField,
      languageField,
      titleClosedField,
      descriptionClosedField,
      alternativeTitleField,
      multiValueWithDependenciesField
    )
  }
}
