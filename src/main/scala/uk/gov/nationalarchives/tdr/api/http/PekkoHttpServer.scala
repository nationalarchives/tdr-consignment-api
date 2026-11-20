package uk.gov.nationalarchives.tdr.api.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.connectors.slick.scaladsl.SlickSession
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import java.nio.file.{Files, Path, Paths}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Using

object PekkoHttpServer {
  def httpsContext(keyStorePath: Path): HttpsConnectionContext = {
    val keyStore = KeyStore.getInstance("PKCS12")
    val password = Array.emptyCharArray
    Using.resource(Files.newInputStream(keyStorePath)) { input =>
      keyStore.load(input, password)
    }
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password)
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, null, null)
    ConnectionContext.httpsServer(sslContext)
  }

  def bindHttps(host: String, port: Int, context: HttpsConnectionContext, route: Route)(implicit system: ActorSystem): Future[Http.ServerBinding] =
    Http().newServerAt(host, port).enableHttps(context).bindFlow(route)
}

class PekkoHttpServer {
  val port = 8080
  val logger: Logger = Logger("PekkoHttpServer")

  val httpsContext = PekkoHttpServer.httpsContext(Paths.get("/api/certs/this.p12"))

  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: Materializer = Materializer(actorSystem)

  import scala.concurrent.duration._

  scala.sys.addShutdownHook(() -> shutdown())

  val slickSession: SlickSession = SlickSession.forConfig("consignmentapi")

  val routes = new Routes(ConfigFactory.load(), slickSession)

  PekkoHttpServer.bindHttps("0.0.0.0", port, httpsContext, routes.route)
  logger.info(s"Consignment API is running")

  def shutdown(): Unit = {
    actorSystem.terminate()
    Await.result(actorSystem.whenTerminated, 30 seconds)
  }
}
