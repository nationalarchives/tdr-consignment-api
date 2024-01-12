package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sttp.client3.SimpleHttpClient
import sttp.client3.testing.SttpBackendStub
import sttp.model.Method

import scala.concurrent.ExecutionContext

class ReferenceGeneratorServiceSpec extends AnyFlatSpec with Matchers {
  implicit val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  case class User(id: String)

  "getReferences" should "return a list of references" in {
    val sttpBackendStub = SttpBackendStub.synchronous
      .whenRequestMatches(_.uri.path.startsWith(List("test", "counter")))
      .thenRespond("""["REF1","REF2"]""")
    val referenceGeneratorService = new ReferenceGeneratorService(ConfigFactory.load, SimpleHttpClient(sttpBackendStub))
    val getReferences = referenceGeneratorService.getReferences(2)

    getReferences shouldBe List("REF1", "REF2")
  }

  "getReferences" should "return the correct number of reference if 'referenceLimit' is exceeded in the request" in {
    val sttpBackendStub = SttpBackendStub.synchronous
      .whenRequestMatches(_.uri.path.startsWith(List("test", "counter")))
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
