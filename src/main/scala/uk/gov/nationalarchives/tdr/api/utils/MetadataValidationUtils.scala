package uk.gov.nationalarchives.tdr.api.utils

import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.CustomMetadataField
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.ClosureType
import uk.gov.nationalarchives.tdr.validation.{DataType, MetadataCriteria, MetadataValidation}

object MetadataValidationUtils {

  def createMetadataValidation(fields: List[CustomMetadataField]): MetadataValidation = {

    val closureFields = fields.filter(f => f.name == ClosureType)
    val descriptiveFields = fields.filter(f => f.propertyGroup.contains("OptionalMetadata"))

    val closureMetadataCriteria = createCriteria(closureFields, fields).head
    val descriptiveMetadataCriteria = createCriteria(descriptiveFields, fields)
    new MetadataValidation(closureMetadataCriteria, descriptiveMetadataCriteria)
  }

  private def createCriteria(customMetadata: List[CustomMetadataField], allCustomMetadata: List[CustomMetadataField]): List[MetadataCriteria] = {
    customMetadata.map(cm => {
      val (definedValues, defaultValue) = cm.dataType match {
        case CustomMetadataFields.Boolean => (List("true", "false"), Some(cm.defaultValue.getOrElse("false")))
        case CustomMetadataFields.Text    => (cm.values.map(_.value), cm.defaultValue)
        case _                            => (List(), cm.defaultValue)
      }
      val requiredField: Boolean = cm.propertyGroup.contains("MandatoryClosure") || cm.propertyGroup.contains("MandatoryMetadata")
      MetadataCriteria(
        cm.name,
        DataType.get(cm.dataType.toString),
        requiredField,
        isFutureDateAllowed = false,
        isMultiValueAllowed = cm.multiValue,
        definedValues,
        defaultValue = defaultValue,
        dependencies = Some(cm.values.map(v => v.value -> createCriteria(allCustomMetadata.filter(m => v.dependencies.exists(_.name == m.name)), allCustomMetadata)).toMap)
      )
    })
  }
}
