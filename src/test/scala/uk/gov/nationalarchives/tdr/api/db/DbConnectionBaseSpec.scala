package uk.gov.nationalarchives.tdr.api.db

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalacache.Entry
import slick.jdbc.JdbcBackend

import scala.jdk.CollectionConverters._

class DbConnectionBaseSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private def newRequest: DbConnectionBase = new DbConnectionBase { override def db: JdbcBackend#Database = null }

  override def beforeEach(): Unit = DbConnectionBase.passwordCache.underlying.invalidateAll()

  "getPassword" should "reuse a password cached by another instance" in {
    // A DbConnection is created for every request, so the cache has to be shared between instances. Otherwise the memoisation never gets a hit and every
    // request generates a new RDS IAM auth token, which blocks the request thread while the AWS credentials are resolved.
    newRequest.getPassword.get should equal("password")

    val cacheKey = DbConnectionBase.passwordCache.underlying.asMap().keySet().asScala.head
    DbConnectionBase.passwordCache.underlying.put(cacheKey, Entry("a previously cached password", None))

    newRequest.getPassword.get should equal("a previously cached password")
  }

  it should "only create a single cache entry regardless of how many instances ask for the password" in {
    (1 to 10).foreach(_ => newRequest.getPassword.get should equal("password"))

    DbConnectionBase.passwordCache.underlying.asMap().asScala should have size 1
  }
}
