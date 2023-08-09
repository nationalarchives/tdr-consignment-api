package uk.gov.nationalarchives.tdr.api.db

import com.typesafe.config.ConfigFactory
import scalacache.CacheConfig
import scalacache.caffeine.CaffeineCache
import scalacache.memoization._
import scalacache.modes.try_._
import slick.jdbc.JdbcBackend
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest

import scala.concurrent.duration._
import scala.util.Try

trait DbConnectionBase {
  implicit val passwordCache: CaffeineCache[String] = CaffeineCache[String](CacheConfig())

  def db: JdbcBackend#DatabaseDef

  def getPassword: Try[String] = memoize[Try, String](Some(5.minutes)) {
    val configFactory = ConfigFactory.load
    val useIamAuth = configFactory.getBoolean("consignmentapi.useIamAuth")
    if (useIamAuth) {
      val rdsClient = RdsUtilities.builder().region(Region.EU_WEST_2).build()
      val port = configFactory.getInt("consignmentapi.db.port")
      val request = GenerateAuthenticationTokenRequest
        .builder()
        .credentialsProvider(DefaultCredentialsProvider.builder().build())
        .hostname(configFactory.getString("consignmentapi.db.host"))
        .port(port)
        .username(configFactory.getString("consignmentapi.db.user"))
        .region(Region.EU_WEST_2)
        .build()
      rdsClient.generateAuthenticationToken(request)
    } else {
      configFactory.getString("consignmentapi.db.password")
    }
  }
}
