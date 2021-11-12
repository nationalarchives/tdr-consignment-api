package uk.gov.nationalarchives.tdr.api.db.repository

import java.sql.Timestamp
import java.util.UUID
import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Body, BodyRow, Consignment, ConsignmentRow, Consignmentstatus, ConsignmentstatusRow, File, Series, SeriesRow}
import uk.gov.nationalarchives.tdr.api.graphql.fields.ConsignmentFields
import uk.gov.nationalarchives.tdr.api.service.TimeSource
import uk.gov.nationalarchives.tdr.api.utils.TimeUtils.ZonedDateTimeUtils

import scala.concurrent.{ExecutionContext, Future}

class ConsignmentRepository(db: Database, timeSource: TimeSource) {

  private val insertQuery = Consignment returning Consignment.map(_.consignmentid) into
    ((consignment, consignmentid) => consignment.copy(consignmentid = consignmentid))

  def addConsignment(consignmentRow: ConsignmentRow): Future[ConsignmentRow] = {
    db.run(insertQuery += consignmentRow)
  }

  def updateExportLocation(exportLocationInput: ConsignmentFields.UpdateExportLocationInput): Future[Int] = {
    //Temporarily generate timestamp until value passed in by clients
    val exportDatetime = exportLocationInput.exportDatetime match {
      case Some(zdt) => zdt.toTimestamp
      case None => Timestamp.from(timeSource.now)
    }

    val update = Consignment.filter(_.consignmentid === exportLocationInput.consignmentId)
      .map(c => (c.exportlocation, c.exportdatetime))
      .update((Option(exportLocationInput.exportLocation), Some(exportDatetime)))
    db.run(update)
  }

  def updateTransferInitiated(consignmentId: UUID, userId: UUID, timestamp: Timestamp): Future[Int] = {
    val update = Consignment.filter(_.consignmentid === consignmentId)
      .map(c => (c.transferinitiateddatetime, c.transferinitiatedby))
      .update((Option(timestamp), Some(userId)))
    db.run(update)
  }

  def getNextConsignmentSequence(implicit executionContext: ExecutionContext): Future[Long] = {
    val query = sql"select nextval('consignment_sequence_id')".as[Long]
    db.run(query).map(result => {
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

  def getConsignments(limit: Int, after: Option[String]): Future[Seq[ConsignmentRow]] = {
    val query = Consignment.filterOpt(after)(_.consignmentreference > _)
      .sortBy(_.consignmentreference)
      .take(limit)
    db.run(query.result)
  }

  def getSeriesOfConsignment(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Seq[SeriesRow]] = {
    val query = Consignment.join(Series)
      .on(_.seriesid === _.seriesid)
      .filter(_._1.consignmentid === consignmentId)
      .map(rows => rows._2)
    db.run(query.result)
  }

  def getTransferringBodyOfConsignment(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Seq[BodyRow]] = {
    val query = Consignment.join(Body)
      .on(_.bodyid === _.bodyid)
      .filter(_._1.consignmentid === consignmentId)
      .map(rows => rows._2)

    db.run(query.result)
  }

  def consignmentHasFiles(consignmentId: UUID): Future[Boolean] = {
    val query = File.filter(_.consignmentid === consignmentId).exists
    db.run(query.result)
  }

  def getConsignmentsOfFiles(fileIds: Seq[UUID])
                            (implicit executionContext: ExecutionContext): Future[Seq[(UUID, ConsignmentRow)]] = {
    val query = for {
      (file, consignment) <- File.join(Consignment).on(_.consignmentid === _.consignmentid).filter(_._1.fileid.inSet(fileIds))
    } yield (file.fileid, consignment)
    db.run(query.result)
  }

  def addParentFolder(consignmentId: UUID, parentFolder: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val updateAction = Consignment.filter(_.consignmentid === consignmentId).map(c => c.parentfolder).update(Option(parentFolder))
    db.run(updateAction).map(_ => ())
  }

  def addParentFolder(consignmentId: UUID, parentFolder: String, consignmentStatusRow: ConsignmentstatusRow)
                     (implicit executionContext: ExecutionContext): Future[String] = {
    val updateAction = Consignment.filter(_.consignmentid === consignmentId).map(c => c.parentfolder).update(Option(parentFolder))
    val consignmentStatusAction = Consignmentstatus += consignmentStatusRow
    val allActions = DBIO.seq(updateAction, consignmentStatusAction).transactionally
    db.run(allActions).map(_ => parentFolder)
  }

  def getParentFolder(consignmentId: UUID)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    val query = Consignment.filter(_.consignmentid === consignmentId).map(_.parentfolder)
    db.run(query.result).map(_.headOption.flatten)
  }

}
