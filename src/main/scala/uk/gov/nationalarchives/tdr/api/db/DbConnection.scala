package uk.gov.nationalarchives.tdr.api.db

import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.typesafe.config.Config
import scalacache.CacheConfig
import scalacache.caffeine.CaffeineCache
import scalacache.memoization._
import scalacache.modes.try_._
import slick.jdbc.JdbcBackend
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
class DbConnection(config: Config) {
  implicit val passwordCache: CaffeineCache[String] = CaffeineCache[String](CacheConfig())
  val slickSession: SlickSession = SlickSession.forConfig("consignmentapi", config)


  def db: JdbcBackend#DatabaseDef = {
    val db = slickSession.db
    db.source match {
      case hikariDataSource: HikariCPJdbcDataSource =>
        val configBean = hikariDataSource.ds.getHikariConfigMXBean
        getPassword match {
          case Failure(exception) => throw exception
          case Success(password) =>
            configBean.setPassword(password)
            db
        }
      case _ =>
        db
    }
  }

  //We've chosen the cache to be 5 minutes. This means that we're not making too many requests to AWS while at the same time
  //it gives us a buffer if anything goes wrong getting the password.
  //IAM database passwords are valid for 15 minutes https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
  def getPassword: Try[String] = memoize[Try, String](Some(5.minutes)) {
    val useIamAuth = config.getBoolean("consignmentapi.useIamAuth")
    if (useIamAuth) {
      val rdsClient = RdsUtilities.builder().region(Region.EU_WEST_2).build()
      val port = config.getInt("consignmentapi.db.port")
      val request = GenerateAuthenticationTokenRequest.builder()
        .credentialsProvider(DefaultCredentialsProvider.builder().build())
        .hostname(config.getString("consignmentapi.db.host"))
        .port(port)
        .username(config.getString("consignmentapi.db.user"))
        .region(Region.EU_WEST_2)
        .build()
      rdsClient.generateAuthenticationToken(request)
    } else {
      config.getString("consignmentapi.db.password")
    }
  }
}

object DbConnection {
  def apply(config: Config) = new DbConnection(config)

}
