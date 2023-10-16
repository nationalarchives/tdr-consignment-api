package uk.gov.nationalarchives.tdr.api.utils

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.testkit.TestDuration
import com.typesafe.config.ConfigFactory
import io.circe.Decoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.unmarshalResponse

import scala.concurrent.duration.DurationInt
import scala.io.Source.fromResource
import scala.reflect.ClassTag

trait TestRequest extends AnyFlatSpec with ScalatestRouteTest with Matchers {

  def runTestRequest[A](prefix: String)(queryFileName: String, token: OAuth2BearerToken)(implicit decoder: Decoder[A], classTag: ClassTag[A]): A = {
    implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(new DurationInt(60).second.dilated(system))
    implicit val unmarshaller: FromResponseUnmarshaller[A] = unmarshalResponse[A]()
    val slickSession = SlickSession.forConfig("consignmentapi")
    val route = new Routes(ConfigFactory.load(), slickSession).route
    val query: String = fromResource(prefix + s"$queryFileName.json").mkString
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(token) ~> route ~> check {
      responseAs[A]
    }
  }
}
