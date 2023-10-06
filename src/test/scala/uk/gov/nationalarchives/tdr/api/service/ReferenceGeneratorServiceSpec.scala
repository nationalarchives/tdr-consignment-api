package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.ConfigFactory
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import sttp.client3.SimpleHttpClient
import sttp.client3.testing.SttpBackendStub
import sttp.model.Method

import scala.concurrent.ExecutionContext

class ReferenceGeneratorServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures with TableDrivenPropertyChecks {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  case class User(id: String)

  "getReferences" should "return a list of references" in {
    val sttpBackendStub = SttpBackendStub.synchronous
      .whenRequestMatches(_.uri.path.startsWith(List("intg", "counter")))
      .thenRespond("""["REF1","REF2"]""")
    val referenceGeneratorService = new ReferenceGeneratorService(ConfigFactory.load, SimpleHttpClient(sttpBackendStub))
    val getReferences = referenceGeneratorService.getReferences(2)

    getReferences shouldBe List("REF1", "REF2")
  }

  "getReferences" should "return a list of references if referenceLimit is exceeded" in {
    val sttpBackendStub = SttpBackendStub.synchronous
      .whenRequestMatches(_.uri.path.startsWith(List("intg", "counter")))
      .thenRespondCyclic("""["REF1","REF2"]""", """["REF3"]""")
    val referenceGeneratorService = new ReferenceGeneratorService(ConfigFactory.load, SimpleHttpClient(sttpBackendStub))
    val getReferences = referenceGeneratorService.getReferences(3)

    getReferences shouldBe List("REF1", "REF2", "REF3")
  }

  "getReferences" should "throw an exception if the call to the reference service fails" in {
    val sttpBackendStub = SttpBackendStub.synchronous
      .whenRequestMatches(_.method == Method.GET)
      .thenRespondServerError()
    val referenceGeneratorService = new ReferenceGeneratorService(ConfigFactory.load, SimpleHttpClient(sttpBackendStub))
    val thrown = intercept[Exception] {
      referenceGeneratorService.getReferences(3)
    }
    assert(thrown.getMessage === "status code: 500, reason phrase: Failed to get references from reference generator api")
  }
}
