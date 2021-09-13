package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables
import uk.gov.nationalarchives.Tables.{Consignmentstatus, ConsignmentstatusRow}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.Future

class ConsignmentStatusRepository(db: Database) {
  private val insertQuery = Consignmentstatus returning Consignmentstatus.map(_.consignmentstatusid) into
    ((consignmentStatus, consignmentstatusid) => consignmentStatus.copy(consignmentstatusid = consignmentstatusid))

  def addConsignmentStatus(consignmentStatusRow: ConsignmentstatusRow): Future[Tables.ConsignmentstatusRow] = {
    db.run(insertQuery += consignmentStatusRow)
  }

  def getConsignmentStatus(consignmentId: UUID): Future[Seq[ConsignmentstatusRow]] = {
    val query = Consignmentstatus.filter(_.consignmentid === consignmentId)
    db.run(query.result)
  }

  def updateConsignmentStatus(consignmentId: UUID, statusType: String, statusValue: String, modifiedTimestamp: Timestamp): Future[Int] = {
    val dbUpdate = Consignmentstatus.filter(cs => cs.consignmentid === consignmentId && cs.statustype === statusType)
      .map(cs => (cs.value, cs.modifieddatetime))
      .update((statusValue, Option(modifiedTimestamp)))
    db.run(dbUpdate)
  }
}
