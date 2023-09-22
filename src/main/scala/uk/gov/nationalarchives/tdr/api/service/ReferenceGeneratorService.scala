package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.{Config, ConfigFactory}
import io.circe.parser._
import sttp.client3.{Response, SimpleHttpClient, UriContext, basicRequest}
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.reference

class ReferenceGeneratorService() {
  private val client: SimpleHttpClient = SimpleHttpClient()
  val config: Config = ConfigFactory.load()
  val environment: String = config.getString("environment")
  val refGeneratorUrl: String = config.getString("referenceGeneratorUrl")

  try {
    val response: Response[Either[String, reference]] = client.send(basicRequest.get(uri"$refGeneratorUrl/$environment/counter?numberofrefs=2"))

    response.body match {
      case Left(body) => println(s"Non-2xx response to GET with code ${response.code}:\n$body")
      case Right(body) =>
        val references = parse(body).getOrElse(null)
        val listOfReferences = references.as[List[reference]].getOrElse(null)
        println(s"2xx response to GET:\n$listOfReferences")
    }

    println("---\n")

  } finally client.close()

}

object ReferenceGeneratorService {
  type reference = String
}
