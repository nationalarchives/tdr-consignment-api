package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Filemetadata, _}
import uk.gov.nationalarchives.tdr.api.graphql.fields.FileMetadataFields.SHA256ServerSideChecksum
import uk.gov.nationalarchives.tdr.api.graphql.fields.MetadataFields._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileMetadataRepository(db: Database)(implicit val executionContext: ExecutionContext) {

  private val insertFileMetadataQuery = Filemetadata returning Filemetadata.map(_.metadataid) into
    ((filemetadata, metadataid) => filemetadata.copy(metadataid = metadataid))

  private val insertFileStatusQuery =
    Filestatus returning Filestatus.map(_.filestatusid) into ((filestatus, filestatusid) => filestatus.copy(filestatusid = filestatusid))

  def getDataType(dataType: Option[String]): DataType = dataType match {
    case Some("text") => Text
    case Some("datetime") => DateTime
    case Some("integer") => Integer
    case Some("decimal") => Decimal
    case _ => throw new Exception(s"Invalid data type $dataType")
  }

  def getCustomMetadata: Future[List[Metadata]] = {
    (for {
      properties <- db.run(Filepropertyv2.result)
      values <- db.run(Filepropertyvaluesv2.result)
      dependencies <- db.run(Filepropertydependanciesv2.result)
    } yield (properties, values, dependencies)).map {
      case (properties, valuesResult, dependenciesResult) =>
        val values: Map[String, Seq[Filepropertyvaluesv2Row]] = valuesResult.groupBy(_.propertyname)
        val dependencies: Map[Int, Seq[Filepropertydependanciesv2Row]] = dependenciesResult.groupBy(_.groupid)

        def rowsToMetadata(fp: Filepropertyv2Row, defaultValueOption: Option[String] = None): Metadata = {
          val metadataValues: Seq[MetadataValues] = values.getOrElse(fp.name, Nil).map(value => {
            value.dependancies.map(groupId => {
              val deps: Seq[Metadata] = for {
                dep <- dependencies.getOrElse(groupId, Nil)
                dependencyProps <- properties.find(_.name == dep.propertyname).map(fp => {
                  rowsToMetadata(fp, dep.default)
                })
              } yield dependencyProps
              MetadataValues(deps.toList, value.propertyvalue)
            }).getOrElse(MetadataValues(Nil, value.propertyvalue))
          })
          Metadata(
            fp.name,
            fp.fullname,
            fp.description,
            fp.propertygroup,
            getDataType(fp.datatype),
            fp.editable.getOrElse(false),
            fp.mutlivalue.getOrElse(false),
            defaultValueOption,
            metadataValues.toList
          )
        }

        properties.map(prop => {
          val defaultValue: Option[String] = for {
            values <- values.get(prop.name)
            value <- values.find(_.default.getOrElse(false))
          } yield value.propertyvalue
          rowsToMetadata(prop, defaultValue)
        }).toList
    }
  }

  def addFileMetadata(rows: Seq[FilemetadataRow]): Future[Seq[FilemetadataRow]] = {
    db.run(insertFileMetadataQuery ++= rows)
  }

  def addChecksumMetadata(fileMetadataRow: FilemetadataRow, fileStatusRow: FilestatusRow): Future[FilemetadataRow] = {
    val allUpdates = DBIO.seq(insertFileMetadataQuery += fileMetadataRow, insertFileStatusQuery += fileStatusRow).transactionally
    db.run(allUpdates).map(_ => fileMetadataRow)
  }

  def getFileMetadata(fileId: UUID, propertyName: String*): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata
      .filter(_.fileid === fileId)
      .filter(_.propertyname inSet propertyName.toSet)
    db.run(query.result)
  }

  def getFileMetadata(consignmentId: UUID): Future[Seq[FilemetadataRow]] = {
    val query = Filemetadata.join(File)
      .on(_.fileid === _.fileid)
      .filter(_._2.consignmentid === consignmentId)
      .map(_._1)
    db.run(query.result)
  }

  def countProcessedChecksumInConsignment(consignmentId: UUID): Future[Int] = {
    val query = Filemetadata.join(File)
      .on(_.fileid === _.fileid).join(Fileproperty)
      .on(_._1.propertyname === _.name)
      .filter(_._1._2.consignmentid === consignmentId)
      .filter(_._2.name === SHA256ServerSideChecksum)
      .groupBy(_._1._2.fileid)
      .map(_._1)
      .length
    db.run(query.result)
  }
}
