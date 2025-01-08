package uk.gov.nationalarchives.tdr.api.service

import com.nimbusds.oauth2.sdk.token.BearerAccessToken
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.circe.parser._
import org.apache.http.client.HttpResponseException
import sttp.client3.{Response, SimpleHttpClient, UriContext, basicRequest}
import uk.gov.nationalarchives.tdr.api.service.ReferenceGeneratorService.Reference
import uk.gov.nationalarchives.tdr.keycloak.Token

import java.util.UUID
import scala.annotation.tailrec

class ReferenceGeneratorService(config: Config, client: SimpleHttpClient) {
  val logger: Logger = Logger("ReferenceGeneratorService")
  private val environment: String = config.getString("environment")
  private val refGeneratorUrl: String = config.getString("referenceGenerator.referenceGeneratorUrl")
  private val refGeneratorLimit: Int = config.getInt("referenceGenerator.referenceLimit")

  def getReferences(numberOfRefs: Int, consignmentId: UUID, token: Token): List[Reference] = {
    @tailrec
    def fetchReferences(numberOfRefs: Int, acc: List[Reference]): List[Reference] = {
      if (numberOfRefs <= 0) acc
      else {
        val batchSize = Math.min(numberOfRefs, refGeneratorLimit)
        fetchReferences(numberOfRefs - batchSize, acc ++ sendRequest(batchSize, consignmentId, token.bearerAccessToken))
      }
    }

    fetchReferences(numberOfRefs, Nil)
  }

  private def sendRequest(numberOfRefs: Int, consignmentId: UUID, bearerAccessToken: BearerAccessToken): List[Reference] = {
    val response: Response[Either[String, Reference]] = client.send(basicRequest
      .auth.bearer(bearerAccessToken.getValue)
      .get(uri"$refGeneratorUrl/$environment/counter?numberofrefs=$numberOfRefs"))

    try {
      response.body match {
        case Left(body) =>
          val message = "Failed to get references from reference generator api"
          logger.error(s"${response.code} $message:\n$body")
          throw new HttpResponseException(response.code.code, message)
        case Right(body) =>
          val references = parse(body).getOrElse(null)
          val listOfReferences = references.as[List[Reference]].getOrElse(null)
          logger.info(s"2xx response to GET:\n$listOfReferences")
          listOfReferences
      }
    } finally {
      client.close()
    }
  }
}

object ReferenceGeneratorService {
  type Reference = String
}
