package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.Config
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.ClosureType
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.validation._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ValidateFileMetadataService(
    customMetadataService: CustomMetadataPropertiesService,
    displayPropertiesService: DisplayPropertiesService,
    fileMetadataRepository: FileMetadataRepository,
    fileStatusRepository: FileStatusRepository,
    config: Config
)(implicit val ec: ExecutionContext) {

  val blockValidationLibrary: Boolean = config.getBoolean("featureAccessBlock.blockValidationLibrary")

  def toPropertyNames(fields: Seq[CustomMetadataField]): Set[String] = fields.map(_.name).toSet

  def toAdditionalMetadataFieldGroups(fields: Seq[CustomMetadataField]): Seq[FieldGroup] = {
    val closureFields = fields.filter(f => f.propertyGroup.contains("MandatoryClosure") || f.propertyGroup.contains("OptionalClosure"))
    val descriptiveFields = fields.filter(f => f.propertyGroup.contains("OptionalMetadata"))
    Seq(FieldGroup(ClosureMetadata, closureFields), FieldGroup(DescriptiveMetadata, descriptiveFields))
  }

  def toValueDependenciesGroups(field: CustomMetadataField): Seq[FieldGroup] = {
    val values: List[CustomMetadataValues] = field.values
    values.map(v => {
      FieldGroup(v.value, v.dependencies)
    })
  }

  def validateAdditionalMetadata(fileIds: Set[UUID], propertiesToValidate: Set[String]): Future[List[FilestatusRow]] = {
    if (blockValidationLibrary) {
      for {
        customMetadataFields <- customMetadataService.getCustomMetadata
        existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(None, Some(fileIds), Some(toPropertyNames(customMetadataFields)))
        rows <- addMetadataFileStatuses(fileIds, propertiesToValidate, customMetadataFields, existingMetadataProperties)
      } yield {
        rows
      }
    } else {
      validateAndAddAdditionalMetadataStatuses(fileIds, propertiesToValidate)
    }
  }

  def validateAndAddAdditionalMetadataStatuses(fileIds: Set[UUID], propertiesToValidate: Set[String]): Future[List[FilestatusRow]] = {
    for {
      propertyNames <- displayPropertiesService.getActiveDisplayPropertyNames
      result <- {
        if (propertiesToValidate.exists(propertyNames.contains)) {
          for {
            additionalMetadataStatuses <- validateAdditionalMetadata(fileIds, propertyNames)
            _ <- fileStatusRepository.deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
            rows <- fileStatusRepository.addFileStatuses(additionalMetadataStatuses)
          } yield rows.toList
        } else {
          Future.successful(Nil)
        }
      }
    } yield {
      result
    }
  }

  private def validateAdditionalMetadata(fileIds: Set[UUID], propertyNames: Seq[String]): Future[List[AddFileStatusInput]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties <- fileMetadataRepository.getFileMetadata(None, Some(fileIds), Some(propertyNames.toSet))
    } yield {
      val additionalMetadataGroups = toAdditionalMetadataFieldGroups(customMetadataFields.filter(p => propertyNames.contains(p.name)))

      val existingFileProperties =
        fileIds.map(fileId => fileId -> existingMetadataProperties.filter(_.fileid == fileId).groupBy(_.propertyname).map(p => p._1 -> p._2.map(_.value).mkString(","))).toMap

      val propertiesToCheck = existingFileProperties.map { case (fileId, properties) =>
        fileId.toString -> propertyNames
          .map(name => {
            val defaultValue = customMetadataFields.find(_.name == name).flatMap(_.defaultValue).getOrElse("")
            Metadata(name, properties.getOrElse(name, defaultValue))
          })
          .toList
      }

      val metadataValidation = MetadataValidationUtils.createMetadataValidation(customMetadataFields.toList)

      val errors = metadataValidation.validateMetadata(propertiesToCheck.map(p => FileRow(p._1, p._2)).toList)

      errors.flatMap { case (fileId, errors) =>
        additionalMetadataGroups
          .map(group => {
            val status = if (errors.exists(error => group.fields.exists(_.name == error.propertyName))) {
              Incomplete
            } else {
              val customMetadata = group.fields.map(field => field.name -> field.defaultValue.getOrElse("")).toMap
              val properties = propertiesToCheck(fileId)
              val hasDefaultValues = customMetadata.forall(p => properties.find(_.name == p._1).exists(_.value == p._2))
              if (hasDefaultValues) {
                NotEntered
              } else {
                Completed
              }
            }
            AddFileStatusInput(UUID.fromString(fileId), group.groupName, status)
          })
      }.toList
    }
  }

  private def addMetadataFileStatuses(
      fileIds: Set[UUID],
      propertiesToValidate: Set[String],
      customMetadataFields: Seq[CustomMetadataField],
      existingMetadataProperties: Seq[Tables.FilemetadataRow]
  ): Future[List[Tables.FilestatusRow]] = {
    val additionalMetadataFieldGroups: Seq[FieldGroup] = toAdditionalMetadataFieldGroups(customMetadataFields)
    val additionalMetadataPropertyNames: Set[String] = additionalMetadataFieldGroups.flatMap(g => toPropertyNames(g.fields)).toSet

    if (!propertiesToValidate.subsetOf(additionalMetadataPropertyNames)) {
      Future.successful(List())
    } else {
      val additionalMetadataStatuses = {

        additionalMetadataFieldGroups
          .flatMap(group => {
            val states = group.fields.flatMap(f => checkPropertyState(fileIds, f, existingMetadataProperties))
            val filesWithNoAdditionalMetadataStatuses = fileIds
              .filter(id => !states.map(_.fileId).contains(id))
              .map(id => {
                AddFileStatusInput(id, group.groupName, NotEntered)
              })

            val statuses = states
              .groupBy(_.fileId)
              .map(s => {
                val (fileId, fileStates) = s
                val status: String = s match {
                  case _ if fileStates.forall(_.existingValueMatchesDefault == true) => NotEntered
                  case _ if fileStates.forall(_.missingDependencies == false)        => Completed
                  case _                                                             => Incomplete
                }
                AddFileStatusInput(fileId, group.groupName, status)
              })
            statuses ++ filesWithNoAdditionalMetadataStatuses
          })
          .toList
      }

      for {
        _ <- fileStatusRepository.deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
        rows <- fileStatusRepository.addFileStatuses(additionalMetadataStatuses)
      } yield rows.toList
    }
  }

  def checkPropertyState(fileIds: Set[UUID], fieldToCheck: CustomMetadataField, existingProperties: Seq[FilemetadataRow]): Seq[FilePropertyState] = {
    val propertyToCheckName: String = fieldToCheck.name
    val valueDependenciesGroups: Seq[FieldGroup] = toValueDependenciesGroups(fieldToCheck)
    val fieldDefaultValue: Option[String] = fieldToCheck.defaultValue
    val dependencyValues: Seq[String] = valueDependenciesGroups.map(_.groupName)

    fileIds
      .flatMap(id => {
        val allExistingFileProperties: Seq[FilemetadataRow] = existingProperties.filter(_.fileid == id)
        val existingPropertiesToValidate: Seq[FilemetadataRow] = allExistingFileProperties.filter(_.propertyname == propertyToCheckName)
        val existingPropertiesWithDependencies: Seq[FilemetadataRow] = existingPropertiesToValidate.filter(ep => dependencyValues.contains(ep.value))
        val expectedDependencies: Seq[CustomMetadataField] =
          existingPropertiesWithDependencies.flatMap(epd => valueDependenciesGroups.filter(_.groupName == epd.value)).flatMap(_.fields)
        val actualDependencyProperties = allExistingFileProperties.filter(p => expectedDependencies.map(_.name).contains(p.propertyname))

        if (existingPropertiesToValidate.isEmpty) {
          None
        } else {
          existingPropertiesToValidate.map(existingProperty => {
            val existingPropertyValue: String = existingProperty.value
            val missingDependencies: Boolean = actualDependencyProperties.size < expectedDependencies.size
            val matchesDefault: Boolean = fieldDefaultValue match {
              case Some(value) => value == existingPropertyValue
              case _           => false
            }
            FilePropertyState(id, propertyToCheckName, missingDependencies, matchesDefault)
          })
        }
      })
      .toSeq
  }

  case class FilePropertyState(fileId: UUID, propertyName: String, missingDependencies: Boolean, existingValueMatchesDefault: Boolean)

  case class FieldGroup(groupName: String, fields: Seq[CustomMetadataField])
}

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
