package uk.gov.nationalarchives.tdr.api.service

import uk.gov.nationalarchives.Tables.FilestatusRow
import uk.gov.nationalarchives.tdr.api.db.repository.{FileMetadataRepository, FileStatusRepository}
import uk.gov.nationalarchives.tdr.api.graphql.fields.CustomMetadataFields.{CustomMetadataField, CustomMetadataValues}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.AddOrUpdateFileMetadata
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileStatusFields.AddFileStatusInput
import uk.gov.nationalarchives.tdr.api.service.FileStatusService._
import uk.gov.nationalarchives.tdr.api.utils.MetadataValidationUtils
import uk.gov.nationalarchives.tdr.validation._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ValidateFileMetadataService(
    customMetadataService: CustomMetadataPropertiesService,
    displayPropertiesService: DisplayPropertiesService,
    fileMetadataRepository: FileMetadataRepository,
    fileStatusRepository: FileStatusRepository
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

  def addAdditionalMetadataStatuses(fileMetadataList: Seq[AddOrUpdateFileMetadata]): Future[Seq[FilestatusRow]] = {
    for {
      customMetadataFields <- customMetadataService.getCustomMetadata
      propertyNames <- displayPropertiesService.getActiveDisplayPropertyNames
      additionalMetadataStatuses = {
        val additionalMetadataGroups = toAdditionalMetadataFieldGroups(customMetadataFields.filter(p => propertyNames.contains(p.name)))
        val metadataGroupsWithDefaultValues =
          additionalMetadataGroups.map(p => p.groupName -> p.fields.map(field => field.name -> field.defaultValue.getOrElse("")).toMap).toMap

        fileMetadataList.flatMap(fileMetadata => {
          metadataGroupsWithDefaultValues.map { case (groupName, fields) =>
            val hasDefaultValues = fields.forall(p => fileMetadata.metadata.find(_.filePropertyName == p._1).exists(_.value == p._2))
            val status = if (hasDefaultValues) {
              NotEntered
            } else {
              Completed
            }
            AddFileStatusInput(fileMetadata.fileId, groupName, status)
          }
        })
      }
      _ <- fileStatusRepository.deleteFileStatus(additionalMetadataStatuses.map(_.fileId).toSet, Set(ClosureMetadata, DescriptiveMetadata))
      rows <- fileStatusRepository.addFileStatuses(additionalMetadataStatuses.toList)
    } yield {
      rows
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

  case class FieldGroup(groupName: String, fields: Seq[CustomMetadataField])
}
