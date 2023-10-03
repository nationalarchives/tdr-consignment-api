package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.circe.parser._
import sttp.client3.{Response, SimpleHttpClient, UriContext, basicRequest}
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.reference

class ReferenceGeneratorService() {
  private val client: SimpleHttpClient = SimpleHttpClient()
  val logger: Logger = Logger("ReferenceGeneratorService")
  val config: Config = ConfigFactory.load()
  val environment: String = config.getString("environment")
  val refGeneratorUrl: String = config.getString("referenceGeneratorUrl")

  def getReferences(numberOfRefs: Int): List[reference] = {
    try {
      val response: Response[Either[String, reference]] = client.send(basicRequest.get(uri"$refGeneratorUrl/$environment/counter?numberofrefs=$numberOfRefs"))
      response.body match {
        case Left(body) => logger.error(s"Non-2xx response to GET with code ${response.code}:\n$body")
        Nil
        case Right(body) =>
          val references = parse(body).getOrElse(null)
          val listOfReferences = references.as[List[reference]].getOrElse(null)
          logger.info(s"2xx response to GET:\n$listOfReferences")
          listOfReferences
      }
    } finally client.close()
  }
}

object ReferenceGeneratorService {
  type reference = String
}
