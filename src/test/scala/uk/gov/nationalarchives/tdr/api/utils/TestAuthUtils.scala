package uk.gov.nationalarchives.tdr.api.utils

import org.apache.pekko.http.scaladsl.model.headers.OAuth2BearerToken
import com.tngtech.keycloakmock.api.{KeycloakMock, ServerConfig}
import com.tngtech.keycloakmock.api.TokenConfig.aTokenConfig

import java.util.UUID

object TestAuthUtils {

  val userId: UUID = UUID.fromString("4ab14990-ed63-4615-8336-56fbb9960300")
  val backendChecksUser: UUID = UUID.fromString("6847253d-b9c6-4ea9-b3c9-57542b8c6375")
  val reportingUser: UUID = UUID.fromString("a863292b-888b-4d88-b5f3-2bb9a11b336a")
  val draftMetadataUser: UUID = UUID.fromString("9b4024e0-61e7-482b-932b-5ea1e0c9d94d")
  val transferServiceUser: UUID = UUID.fromString("5be4be46-cbd3-4073-8500-3be04522145d")

  private val tdrPort: Int = 8000
  private val testPort: Int = 8001
  private val tdrMock: KeycloakMock = createServer("tdr", tdrPort)
  private val testMock: KeycloakMock = createServer("test", testPort)

  def validUserToken(userId: UUID = userId, body: String = "Code", standardUser: String = "true"): OAuth2BearerToken =
    OAuth2BearerToken(
      tdrMock.getAccessToken(
        aTokenConfig()
          .withResourceRole("tdr", "tdr_user")
          .withClaim("body", body)
          .withClaim("user_id", userId)
          .withClaim("standard_user", standardUser)
          .build
      )
    )

  def validJudgmentUserToken(userId: UUID = userId, body: String = "Code", judgmentUser: String = "true"): OAuth2BearerToken =
    OAuth2BearerToken(
      tdrMock.getAccessToken(
        aTokenConfig()
          .withResourceRole("tdr", "tdr_user")
          .withClaim("body", body)
          .withClaim("user_id", userId)
          .withClaim("judgment_user", judgmentUser)
          .build
      )
    )

  def validTNAUserToken(userId: UUID = userId, body: String = "Code", tnaUserType: String = "tna_user", tnaUser: String = "true"): OAuth2BearerToken =
    OAuth2BearerToken(
      tdrMock.getAccessToken(
        aTokenConfig()
          .withResourceRole("tdr", "tdr_user")
          .withClaim("body", body)
          .withClaim("user_id", userId)
          .withClaim(tnaUserType, tnaUser)
          .build
      )
    )

  def validUserTokenNoBody: OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr", "tdr_user")
        .withClaim("user_id", userId)
        .build
    )
  )

  def validBackendChecksToken(role: String): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr-backend-checks", role)
        .withClaim("user_id", backendChecksUser)
        .build
    )
  )

  def validDraftMetadataToken(role: String): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr-draft-metadata", role)
        .withClaim("user_id", draftMetadataUser)
        .build
    )
  )

  def validTransferServiceToken(role: String): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr-transfer-service", role)
        .withClaim("user_id", transferServiceUser)
        .build()
    )
  )

  def invalidBackendChecksToken(): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withClaim("user_id", backendChecksUser)
        .withResourceRole("tdr-backend-checks", "some_role")
        .build
    )
  )

  def validReportingToken(role: String): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr-reporting", role)
        .withClaim("user_id", reportingUser)
        .build
    )
  )

  def invalidReportingToken(): OAuth2BearerToken = OAuth2BearerToken(
    tdrMock.getAccessToken(
      aTokenConfig()
        .withClaim("user_id", reportingUser)
        .withResourceRole("tdr-reporting", "some_role")
        .build
    )
  )

  def invalidToken: OAuth2BearerToken = OAuth2BearerToken(testMock.getAccessToken(aTokenConfig().build))

  private def createServer(realm: String, port: Int): KeycloakMock = {
    val config = ServerConfig.aServerConfig().withPort(port).withDefaultRealm(realm).build()
    val mock: KeycloakMock = new KeycloakMock(config)
    mock.start()
    mock
  }
}
