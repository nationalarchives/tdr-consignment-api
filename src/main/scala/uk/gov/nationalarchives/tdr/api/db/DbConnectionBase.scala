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

object DbConnectionBase {
  // A new DbConnection is created for every request, so these are held here rather than on the instance. Otherwise each request gets an empty cache and generates a new
  // IAM auth token, which blocks the request thread while the AWS credentials are resolved.
  private[db] implicit val passwordCache: CaffeineCache[String] = CaffeineCache[String](CacheConfig())
  private[db] lazy val credentialsProvider: DefaultCredentialsProvider = DefaultCredentialsProvider.builder().build()
  private[db] lazy val rdsUtilities: RdsUtilities = RdsUtilities.builder().region(Region.EU_WEST_2).build()
}

trait DbConnectionBase {
  import DbConnectionBase._

  def db: JdbcBackend#Database

  def getPassword: Try[String] = memoize[Try, String](Some(5.minutes)) {
    val configFactory = ConfigFactory.load
    val useIamAuth = configFactory.getBoolean("consignmentapi.useIamAuth")
    if (useIamAuth) {
      val port = configFactory.getInt("consignmentapi.db.port")
      val request = GenerateAuthenticationTokenRequest
        .builder()
        .credentialsProvider(credentialsProvider)
        .hostname(configFactory.getString("consignmentapi.db.host"))
        .port(port)
        .username(configFactory.getString("consignmentapi.db.user"))
        .region(Region.EU_WEST_2)
        .build()
      rdsUtilities.generateAuthenticationToken(request)
    } else {
      configFactory.getString("consignmentapi.db.password")
    }
  }
}
