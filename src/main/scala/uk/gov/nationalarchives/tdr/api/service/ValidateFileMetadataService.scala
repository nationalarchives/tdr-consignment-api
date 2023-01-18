package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues}
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ValidateFileMetadataService(
    customMetadataService: CustomMetadataPropertiesService,
    fileMetadataRepository: FileMetadataRepository,
    fileStatusRepository: FileStatusRepository,
    timeSource: TimeSource
)(implicit val ec: ExecutionContext) {

  implicit class CustomMetadataFieldsHelper(fields: Seq[CustomMetadataField]) {

    case class FieldGroup(groupName: String, fields: Seq[CustomMetadataField])

    def toPropertyNames: Set[String] = fields.map(_.name).toSet

    def toAdditionalMetadataFieldGroups: Seq[FieldGroup] = {
      Seq(FieldGroup(ClosureMetadata, closureFields), FieldGroup(DescriptiveMetadata, descriptiveFields))
    }

    def closureFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("MandatoryClosure") || f.propertyGroup.contains("OptionalClosure"))
    }

    def descriptiveFields: Seq[CustomMetadataField] = {
      fields.filter(f => f.propertyGroup.contains("OptionalMetadata"))
    }

    def toValueDependenciesGroups: Seq[FieldGroup] = {
      fields
        .flatMap(f => {
          val values: List[CustomMetadataValues] = f.values
          values.map(v => {
            FieldGroup(v.value, v.dependencies)
          })
        })
    }
  }

  def validateAdditionalMetadata(fileIds: Set[UUID], consignmentId: UUID, propertiesToValidate: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(customMetadataFields.toPropertyNames))
    } yield {
      val additionalMetadataGroups: Seq[CustomMetadataFieldsHelper#FieldGroup] = customMetadataFields.toAdditionalMetadataFieldGroups
      val additionalMetadataProperties: Set[String] = additionalMetadataGroups.flatMap(_.fields.toPropertyNames).toSet

      if (!propertiesToValidate.subsetOf(additionalMetadataProperties)) {
        List()
      } else {
        val additionalMetadataStatuses = {
          additionalMetadataGroups
            .flatMap(group => {
              val states = group.fields.flatMap(f => checkPropertyState(fileIds, f, existingMetadataProperties))
              val filesWithNoAdditionalMetadataStatuses = fileIds
                .filter(id => !states.map(_.fileId).contains(id))
                .map(id => {
                  FilestatusRow(UUID.randomUUID(), id, group.groupName, NotEntered, Timestamp.from(timeSource.now))
                })

              val statuses = states
                .groupBy(_.fileId)
                .map(s => {
                  val fileId = s._1
                  val status: String = s match {
                    case s if allPropertiesCompleted(s._2)  => Completed
                    case s if allPropertiesNotEntered(s._2) => NotEntered
                    case _                                  => Incomplete
                  }
                  FilestatusRow(UUID.randomUUID(), fileId, group.groupName, status, Timestamp.from(timeSource.now))
                })
              statuses ++ filesWithNoAdditionalMetadataStatuses
            })
            .toList
        }

        fileStatusRepository.deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
        fileStatusRepository.addFileStatuses(additionalMetadataStatuses)
        additionalMetadataStatuses
      }
    }
  }

  private def allPropertiesCompleted(states: Seq[FilePropertyState]): Boolean = {
    states.forall(_.missingDependencies == false) && states.exists(_.existingValueMatchesDefault.contains(false))
  }

  private def allPropertiesNotEntered(states: Seq[FilePropertyState]): Boolean = {
    states.forall(_.existingValueMatchesDefault.contains(true))
  }

  def checkPropertyState(fileIds: Set[UUID], fieldToCheck: CustomMetadataField, existingProperties: Seq[FilemetadataRow]): Seq[FilePropertyState] = {
    val propertyToCheckName: String = fieldToCheck.name
    val valueDependenciesGroups = Seq(fieldToCheck).toValueDependenciesGroups
    val fieldDefaultValue: Option[String] = fieldToCheck.defaultValue

    fileIds
      .flatMap(id => {
        val allExistingFileProperties: Seq[FilemetadataRow] = existingProperties.filter(_.fileid == id)
        val existingPropertiesToValidate = allExistingFileProperties.filter(_.propertyname == propertyToCheckName)
        if (existingPropertiesToValidate.isEmpty) {
          None
        } else {
          existingPropertiesToValidate.map(existingProperty => {
            val existingPropertyValue: String = existingProperty.value
            val valueDependencies = valueDependenciesGroups.filter(_.groupName == existingPropertyValue).toSet

            // Validity test will need to change if multiple value fields require a set of dependencies for each value, eg
            // FOIExemptionCode 1 requires ClosurePeriod 1
            // FOIExemptionCode 2 requires ClosurePeriod 2 etc
            val missingDependencies: Boolean = !valueDependencies.flatMap(_.fields.toPropertyNames).subsetOf(allExistingFileProperties.map(_.propertyname).toSet)
            val matchesDefault: Option[Boolean] = fieldDefaultValue match {
              case Some(value) => Some(value == existingPropertyValue)
              case _           => None
            }

            FilePropertyState(id, propertyToCheckName, missingDependencies, matchesDefault)
          })
        }
      })
      .toSeq
  }

  case class FilePropertyState(fileId: UUID, propertyName: String, missingDependencies: Boolean, existingValueMatchesDefault: Option[Boolean])
}
