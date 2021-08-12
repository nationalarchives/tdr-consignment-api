package uk.gov.nationalarchives.tdr.api.utils

import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import com.typesafe.config.ConfigFactory
import io.circe.Decoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import uk.gov.nationalarchives.tdr.api.http.Routes
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.unmarshalResponse

import scala.io.Source.fromResource
import scala.reflect.ClassTag

trait TestRequest extends AnyFlatSpec with ScalatestRouteTest with Matchers {

  val route = new Routes(ConfigFactory.load()).route

  def runTestRequest[A](prefix: String)(queryFileName: String, token: OAuth2BearerToken)
                       (implicit decoder: Decoder[A], classTag: ClassTag[A])
  : A = {
    implicit val unmarshaller: FromResponseUnmarshaller[A] = unmarshalResponse[A]()
    val query: String = fromResource(prefix + s"$queryFileName.json").mkString
    Post("/graphql").withEntity(ContentTypes.`application/json`, query) ~> addCredentials(token) ~> route ~> check {
      responseAs[A]
    }
  }

}
