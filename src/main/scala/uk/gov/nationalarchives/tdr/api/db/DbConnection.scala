package uk.gov.nationalarchives.tdr.api.db

import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.typesafe.config.{Config, ConfigFactory}
import scalacache.CacheConfig
import scalacache.caffeine.CaffeineCache
import scalacache.memoization._
import scalacache.modes.try_._
import slick.jdbc.JdbcBackend
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials, DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.util.UUID
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object DbConnection {
  implicit val passwordCache: CaffeineCache[String] = CaffeineCache[String](CacheConfig())
  val slickSession: SlickSession = SlickSession.forConfig("consignmentapi")
  val slickSessionWithConfig: Config => SlickSession = config => SlickSession.forConfig("consignmentapi", config)

  def db(config: Config): JdbcBackend#DatabaseDef = {
    val db = slickSessionWithConfig(config).db
    db.source match {
      case hikariDataSource: HikariCPJdbcDataSource =>
        val configBean = hikariDataSource.ds.getHikariConfigMXBean
        getPassword(config) match {
          case Failure(exception) => throw exception
          case Success(password) =>
            configBean.setPassword(password)
            db
        }
      case _ =>
        db
    }
  }

  def db: JdbcBackend#DatabaseDef = {
    val db = slickSession.db
    db.source match {
      case hikariDataSource: HikariCPJdbcDataSource =>
        val configBean = hikariDataSource.ds.getHikariConfigMXBean
        getPassword(ConfigFactory.load()) match {
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
  def getPassword(config: Config): Try[String] = memoize[Try, String](Some(5.minutes)) {
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
