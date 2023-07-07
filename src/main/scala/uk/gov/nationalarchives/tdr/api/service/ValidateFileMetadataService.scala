package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives
import uk.gov.nationalarchives.Tables
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
    timeSource: TimeSource,
    uuidSource: UUIDSource
)(implicit val ec: ExecutionContext) {

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

  def validateAdditionalMetadata(fileIds: Set[UUID], consignmentId: UUID, propertiesToValidate: Set[String]): Future[List[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      existingMetadataProperties: Seq[FilemetadataRow] <- fileMetadataRepository.getFileMetadata(Some(consignmentId), Some(fileIds), Some(toPropertyNames(customMetadataFields)))
      rows <- addMetadataFileStatuses(fileIds, propertiesToValidate, customMetadataFields, existingMetadataProperties)
    } yield {
      rows
    }
  }

  private def addMetadataFileStatuses(
      fileIds: Set[UUID],
      propertiesToValidate: Set[String],
      customMetadataFields: Seq[CustomMetadataField],
      existingMetadataProperties: Seq[Tables.FilemetadataRow]
  ): Future[List[nationalarchives.Tables.FilestatusRow]] = {
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
                FilestatusRow(uuidSource.uuid, id, group.groupName, NotEntered, Timestamp.from(timeSource.now))
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
                FilestatusRow(uuidSource.uuid, fileId, group.groupName, status, Timestamp.from(timeSource.now))
              })
            statuses ++ filesWithNoAdditionalMetadataStatuses
          })
          .toList
      }

      for {
        _ <- fileStatusRepository.deleteFileStatus(fileIds, Set(ClosureMetadata, DescriptiveMetadata))
        _ <- fileStatusRepository.addFileStatuses(additionalMetadataStatuses)
      } yield additionalMetadataStatuses

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
