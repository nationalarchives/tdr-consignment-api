package uk.gov.nationalarchives.tdr.api.service

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.circe.parser._
import org.apache.http.client.HttpResponseException
import sttp.client3.{Response, SimpleHttpClient, UriContext, basicRequest}
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.reference

import scala.annotation.tailrec

class ReferenceGeneratorService(config: Config, client: SimpleHttpClient) {
  val logger: Logger = Logger("ReferenceGeneratorService")
  val environment: String = config.getString("environment")
  val refGeneratorUrl: String = config.getString("referenceGenerator.referenceGeneratorUrl")
  val refGeneratorLimit: Int = config.getInt("referenceGenerator.referenceLimit")

  def getReferences(numberOfRefs: Int): List[reference] = {

    def processResponse(response: Response[Either[String, reference]]): List[reference] = {
      try {
        response.body match {
          case Left(body) =>
            logger.error(s"Non-2xx response to GET with code ${response.code}:\n$body")
            throw new HttpResponseException(response.code.code, "Failed to get references from reference generator api")
          case Right(body) =>
            val references = parse(body).getOrElse(null)
            val listOfReferences = references.as[List[reference]].getOrElse(null)
            logger.info(s"2xx response to GET:\n$listOfReferences")
            listOfReferences
        }
      } finally {
        client.close()
      }
    }

    @tailrec
    def recursivelyFetchReferences(numberOfRefs: Int, acc: List[reference]): List[reference] = {
      if (numberOfRefs <= 0) acc
      else {
        val batchSize = Math.min(numberOfRefs, refGeneratorLimit)
        val response: Response[Either[String, reference]] = client.send(basicRequest.get(uri"$refGeneratorUrl/$environment/counter?numberofrefs=$batchSize"))
        recursivelyFetchReferences(numberOfRefs - batchSize, acc ++ processResponse(response))
      }
    }

    if (numberOfRefs > refGeneratorLimit) {
      recursivelyFetchReferences(numberOfRefs, Nil)
    } else {
      try {
        val response: Response[Either[String, reference]] = client.send(basicRequest.get(uri"$refGeneratorUrl/$environment/counter?numberofrefs=$numberOfRefs"))
        processResponse(response)
      } finally client.close()
    }
  }
}

object ReferenceGeneratorService {
  type reference = String
}
