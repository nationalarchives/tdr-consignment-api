package uk.gov.nationalarchives.tdr.api.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.{HttpURLConnection, SocketTimeoutException, URI}
import java.nio.file.{Files, Paths}
import java.security.KeyStore
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManagerFactory}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Using

class PekkoHttpServerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem("https-server-test")

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  "The server" should "accept HTTPS and reject plaintext HTTP" in {
    val keyStorePath = Paths.get(getClass.getResource("/https-test.p12").toURI)
    val context = PekkoHttpServer.httpsContext(keyStorePath)
    val binding = Await.result(PekkoHttpServer.bindHttps("127.0.0.1", 0, context, complete("OK")), 10.seconds)

    try {
      val trustStore = KeyStore.getInstance("PKCS12")
      Using.resource(Files.newInputStream(keyStorePath)) { input =>
        trustStore.load(input, Array.emptyCharArray)
      }
      val trustManager = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trustManager.init(trustStore)
      val clientContext = SSLContext.getInstance("TLS")
      clientContext.init(null, trustManager.getTrustManagers, null)

      val port = binding.localAddress.getPort
      val https = URI.create(s"https://localhost:$port/").toURL.openConnection().asInstanceOf[HttpsURLConnection]
      https.setSSLSocketFactory(clientContext.getSocketFactory)
      https.setConnectTimeout(5000)
      https.setReadTimeout(5000)
      try https.getResponseCode shouldBe 200
      finally https.disconnect()

      val http = URI.create(s"http://localhost:$port/").toURL.openConnection().asInstanceOf[HttpURLConnection]
      http.setConnectTimeout(5000)
      http.setReadTimeout(5000)
      try {
        val error = intercept[IOException](http.getResponseCode)
        error should not be a[SocketTimeoutException]
      } finally http.disconnect()
    } finally {
      Await.result(binding.unbind(), 10.seconds)
    }
  }
}
