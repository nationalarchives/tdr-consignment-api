package uk.gov.nationalarchives.tdr.api.service

import java.util.UUID

import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.Tables.BodyRow
import uk.gov.nationalarchives.tdr.api.db.repository.TransferringBodyRepository

import scala.concurrent.{ExecutionContext, Future}

class TransferringBodyServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers {

  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  val repository: TransferringBodyRepository = mock[TransferringBodyRepository]
  val service = new TransferringBodyService(repository)

  "getBody" should "return a transferring body matching the series ID" in {
    val seriesId = UUID.fromString("20e88b3c-d063-4a6e-8b61-187d8c51d11d")
    val bodyId = UUID.fromString("8a72cc59-7f2f-4e55-a263-4a4cb9f677f5")

    val bodyRow = BodyRow(bodyId, Some("Some department name"), Some("CODE"))
    when(repository.getTransferringBody(seriesId)).thenReturn(Future.successful(bodyRow))

    val body = service.getBody(seriesId)

    body.futureValue.tdrCode.get should equal("CODE")
  }
}
