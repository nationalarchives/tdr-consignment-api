package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.JdbcBackend
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Body, BodyRow, Consignment, ConsignmentRow, Consignmentstatus, ConsignmentstatusRow, File, Series, SeriesRow}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields.{ConsignmentFilters, StartUploadInput, UpdateMetadataSchemaLibraryVersionInput}
import uk.gov.nationalarchives.tdr.api.service.TimeSource
import uk.gov.nationalarchives.tdr.api.utils.Statuses.{InProgressValue, MetadataReviewType}
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.ZonedDateTimeUtils

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ConsignmentRepository(db: JdbcBackend#Database, timeSource: TimeSource) {

  private val insertQuery = Consignment returning Consignment.map(_.consignmentid) into
    ((consignment, consignmentid) => consignment.copy(consignmentid = consignmentid))

  def addConsignment(consignmentRow: ConsignmentRow): Future[ConsignmentRow] = {
    db.run(insertQuery += consignmentRow)
  }

  def updateExportData(exportDataInput: ConsignmentFields.UpdateExportDataInput): Future[Int] = {
    // Temporarily generate timestamp until value passed in by clients
    val exportDatetime = exportDataInput.exportDatetime match {
      case Some(zdt) => zdt.toTimestamp
      case None      => Timestamp.from(timeSource.now)
    }

    val update = Consignment
      .filter(_.consignmentid === exportDataInput.consignmentId)
      .map(c => (c.exportlocation, c.exportdatetime, c.exportversion))
      .update((Option(exportDataInput.exportLocation), Some(exportDatetime), Option(exportDataInput.exportVersion)))
    db.run(update)
  }

  def updateTransferInitiated(consignmentId: UUID, userId: UUID, timestamp: Timestamp): Future[Int] = {
    val update = Consignment
      .filter(_.consignmentid === consignmentId)
      .map(c => (c.transferinitiateddatetime, c.transferinitiatedby))
      .update((Option(timestamp), Some(userId)))
    db.run(update)
  }

  def getNextConsignmentSequence(implicit executionContext: ExecutionContext): Future[Long] = {
    val query = sql"select nextval('consignment_sequence_id')".as[Long]
    db.run(query)
      .map(result => {
        if (result.size == 1) {
          result.head
        } else {
          throw new IllegalStateException(s"Expected single consignment sequence value but got: ${result.size} values instead.")
        }
      })
  }

  def getConsignment(consignmentId: UUID): Future[Seq[ConsignmentRow]] = {
    val query = Consignment.filter(_.consignmentid === consignmentId)
    db.run(query.result)
  }

  def getConsignmentsForMetadataReview: Future[Seq[ConsignmentRow]] = {
    val query = Consignment
      .join(Consignmentstatus)
      .on(_.consignmentid === _.consignmentid)
      .filter(_._2.statustype === MetadataReviewType.id)
      .filter(_._2.value === InProgressValue.value)
      .map(_._1)
    db.run(query.result)
  }

  def getConsignmentForMetadataReview(consignmentId: UUID): Future[Seq[ConsignmentRow]] = {
    val query = Consignment
      .join(Consignmentstatus)
      .on(_.consignmentid === _.consignmentid)
      .filter(_._1.consignmentid === consignmentId)
      .filter(_._2.statustype === MetadataReviewType.id)
      .filter(_._2.value === InProgressValue.value)
      .map(_._1)
    db.run(query.result)
  }

  def getConsignments(limit: Int, after: Option[String], currentPage: Option[Int] = None, consignmentFilters: Option[ConsignmentFilters] = None): Future[Seq[ConsignmentRow]] = {
    val offset = currentPage.map(_ * limit).getOrElse(0)
    val query = Consignment
      .filterOpt(consignmentFilters.flatMap(_.userId))(_.userid === _)
      .filterOpt(consignmentFilters.flatMap(_.consignmentType))(_.consignmenttype === _)
      .sortBy(_.consignmentreference.desc.nullsFirst)
      .filterOpt(after)(_.consignmentreference < _)
      .drop(offset)
      .take(limit)
    db.run(query.result)
  }

  def getTotalConsignments(consignmentFilters: Option[ConsignmentFilters]): Future[Int] = {
    val query = Consignment
      .filterOpt(consignmentFilters.flatMap(_.userId))(_.userid === _)
      .filterOpt(consignmentFilters.flatMap(_.consignmentType))(_.consignmenttype === _)
      .length
    db.run(query.result)
  }

  def getSeriesOfConsignment(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Seq[SeriesRow]] = {
    val query = Consignment
      .join(Series)
      .on(_.seriesid === _.seriesid)
      .filter(_._1.consignmentid === consignmentId)
      .map(rows => rows._2)
    db.run(query.result)
  }

  def updateSeriesOfConsignment(updateConsignmentSeriesIdInput: ConsignmentFields.UpdateConsignmentSeriesIdInput, seriesName: Option[String]): Future[Int] = {
    val update = Consignment
      .filter(_.consignmentid === updateConsignmentSeriesIdInput.consignmentId)
      .map(t => (t.seriesid, t.seriesname))
      .update(Some(updateConsignmentSeriesIdInput.seriesId), seriesName)
    db.run(update)
  }

  def getTransferringBodyOfConsignment(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Seq[BodyRow]] = {
    val query = Consignment
      .join(Body)
      .on(_.bodyid === _.bodyid)
      .filter(_._1.consignmentid === consignmentId)
      .map(rows => rows._2)

    db.run(query.result)
  }

  def consignmentHasFiles(consignmentId: UUID): Future[Boolean] = {
    val query = File.filter(_.consignmentid === consignmentId).exists
    db.run(query.result)
  }

  def getConsignmentsOfFiles(fileIds: Seq[UUID])(implicit executionContext: ExecutionContext): Future[Seq[(UUID, ConsignmentRow)]] = {
    val query = for {
      (file, consignment) <- File.join(Consignment).on(_.consignmentid === _.consignmentid).filter(_._1.fileid.inSet(fileIds))
    } yield (file.fileid, consignment)
    db.run(query.result)
  }

  def addUploadDetails(uploadInput: StartUploadInput, consignmentStatusRows: List[ConsignmentstatusRow])(implicit executionContext: ExecutionContext): Future[String] = {
    val updateAction =
      Consignment
        .filter(_.consignmentid === uploadInput.consignmentId)
        .map(c => (c.parentfolder, c.includetoplevelfolder))
        .update((Option(uploadInput.parentFolder), Option(uploadInput.includeTopLevelFolder)))
    val consignmentStatusAction = Consignmentstatus ++= consignmentStatusRows
    val allActions = DBIO.seq(updateAction, consignmentStatusAction).transactionally
    db.run(allActions).map(_ => uploadInput.parentFolder)
  }

  def getParentFolder(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    val query = Consignment.filter(_.consignmentid === consignmentId).map(_.parentfolder)
    db.run(query.result).map(_.headOption.flatten)
  }

  def updateMetadataSchemaLibraryVersion(updateMetadataSchemaLibraryVersionInput: UpdateMetadataSchemaLibraryVersionInput): Future[Int] = {
    val update = Consignment
      .filter(_.consignmentid === updateMetadataSchemaLibraryVersionInput.consignmentId)
      .map(_.metadataschemalibraryversion)
      .update(Some(updateMetadataSchemaLibraryVersionInput.metadataSchemaLibraryVersion))
    db.run(update)
  }
}
