package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.{FilemetadataRow, FilestatusRow}
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues}
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
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

    private val closureFieldNames: Set[String] = Set(
      ClosureType,
      ClosureStartDate,
      ClosurePeriod,
      FoiExemptionCode.name,
      FoiExemptionAsserted,
      TitleClosed,
      TitleAlternate,
      DescriptionAlternate,
      DescriptionClosed
    )
    private val descriptiveFieldNames: Set[String] = Set(Description, Language.name)

    case class FieldGroup(groupName: String, fields: Seq[CustomMetadataField])

    def toPropertyNames: Set[String] = fields.map(_.name).toSet

    def toCustomMetadataFieldGroups: Seq[FieldGroup] = {
      Seq(FieldGroup(ClosureMetadata, closureFields), FieldGroup(DescriptiveMetadata, descriptiveFields))
    }

    def closureFields: Seq[CustomMetadataField] = {
      fields.filter(f => closureFieldNames.contains(f.name))
    }

    def descriptiveFields: Seq[CustomMetadataField] = {
      fields.filter(f => descriptiveFieldNames.contains(f.name))
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

  def validateFileMetadata(fileIds: Set[UUID], consignmentId: UUID, properties: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(consignmentId, Some(fileIds), Some(customMetadataFields.toPropertyNames))
    } yield {
      val fieldGroups: Seq[CustomMetadataFieldsHelper#FieldGroup] = customMetadataFields.toCustomMetadataFieldGroups
      val groupsProperties: Set[String] = fieldGroups.flatMap(_.fields.toPropertyNames).toSet

      if (!properties.subsetOf(groupsProperties)) {
        List()
      } else {
        val allFileStatuses = fieldGroups
          .flatMap(group => {
            val states = group.fields.flatMap(f => validateProperty(fileIds, f, existingMetadataProperties))

            val statelessStatuses = fileIds
              .filter(id => !states.map(_.fileId).contains(id))
              .map(id => {
                FilestatusRow(UUID.randomUUID(), id, group.groupName, NotEntered, Timestamp.from(timeSource.now))
              })

            val statuses = states
              .groupBy(_.fileId)
              .map(s => {
                val status: String = s match {
                  case s if s._2.forall(_.valid == true) && s._2.exists(_.existingValueMatchesDefault.contains(false)) => Completed
                  case s if s._2.forall(_.existingValueMatchesDefault.contains(true))                                  => NotEntered
                  case _                                                                                               => Incomplete
                }
                FilestatusRow(UUID.randomUUID(), s._1, group.groupName, status, Timestamp.from(timeSource.now))
              })

            statuses ++ statelessStatuses

          })
          .toList

        fileStatusRepository.deleteFileStatus(fileIds, allFileStatuses.map(_.statustype).toSet)
        fileStatusRepository.addFileStatuses(allFileStatuses)

        allFileStatuses
      }
    }
  }

  def validateProperty(fileIds: Set[UUID], fieldToValidate: CustomMetadataField, existingProperties: Seq[FilemetadataRow]): Seq[FilePropertyState] = {
    val propertyToValidateName: String = fieldToValidate.name
    val valueDependenciesGroups = Seq(fieldToValidate).toValueDependenciesGroups
    val fieldDefaultValue: Option[String] = fieldToValidate.defaultValue

    fileIds
      .flatMap(id => {
        val allExistingFileProperties: Seq[FilemetadataRow] = existingProperties.filter(_.fileid == id)
        val existingPropertiesToValidate = allExistingFileProperties.filter(_.propertyname == propertyToValidateName)
        if (existingPropertiesToValidate.isEmpty) {
          None
        } else {
          existingPropertiesToValidate.map(existingProperty => {
            val existingPropertyValue: String = existingProperty.value
            val valueDependencies = valueDependenciesGroups.filter(_.groupName == existingPropertyValue).toSet

            // Validity test will need to change if multiple value fields require a set of dependencies for each value, eg
            // FOIExemptionCode 1 requires ClosurePeriod 1
            // FOIExemptionCode 2 requires ClosurePeriod 2 etc
            val valid: Boolean = valueDependencies.flatMap(_.fields.toPropertyNames).subsetOf(allExistingFileProperties.map(_.propertyname).toSet)
            val matchesDefault: Option[Boolean] = fieldDefaultValue match {
              case Some(value) => Some(value == existingPropertyValue)
              case _           => None
            }

            FilePropertyState(id, propertyToValidateName, valid, matchesDefault)
          })
        }
      })
      .toSeq
  }

  case class FilePropertyState(fileId: UUID, propertyName: String, valid: Boolean, existingValueMatchesDefault: Option[Boolean])
}
