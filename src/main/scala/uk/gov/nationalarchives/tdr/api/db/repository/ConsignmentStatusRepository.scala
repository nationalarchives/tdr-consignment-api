package uk.gov.nationalarchives.tdr.api.db.repository

import slick.jdbc.PostgresProfile.api._
import uk.gov.nationalarchives.Tables.{Consignmentstatus, ConsignmentstatusRow}

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.Future

class ConsignmentStatusRepository(db: Database) {

  def getConsignmentStatus(consignmentId: UUID): Future[Seq[ConsignmentstatusRow]] = {
    val query = Consignmentstatus.filter(_.consignmentid === consignmentId)
    db.run(query.result)
  }

  def setUploadConsignmentStatusValueToComplete(consignmentId: UUID, statusType: String, statusValue: String, modifiedTimestamp: Timestamp): Future[Int] = {
    val dbUpdate = Consignmentstatus.filter(_.consignmentid === consignmentId)
      .map(cs => (cs.statustype, cs.value, cs.modifieddatetime))
      .update((statusType, statusValue, Option(modifiedTimestamp)))
    db.run(dbUpdate)
  }
}
