package uk.gov.nationalarchives.tdr.api.service

import java.util.UUID

import uk.gov.nationalarchives.tdr.api.db.repository.TransferringBodyRepository
import uk.gov.nationalarchives.tdr.api.graphql.DataExceptions.InputDataException
import uk.gov.nationalarchives.tdr.api.model.TransferringBody

import scala.concurrent.{ExecutionContext, Future}

class TransferringBodyService(transferringBodyRepository: TransferringBodyRepository) {

  def getBody(seriesId: UUID)(implicit executionContext: ExecutionContext): Future[TransferringBody] = {
    val bodyRow = transferringBodyRepository.getTransferringBody(seriesId)
    bodyRow.map(body => TransferringBody(body.tdrcode, body.bodyid))
  }

  def getBodyByCode(tdrCode: String)(implicit executionContext: ExecutionContext): Future[TransferringBody] = {
    val bodyRow = transferringBodyRepository.getTransferringBodyByCode(tdrCode)
    bodyRow.map {
      case Some(value) => TransferringBody(value.tdrcode, value.bodyid)
      case _ => throw InputDataException(s"No transferring body found for code '$tdrCode'")
    }
  }
}
