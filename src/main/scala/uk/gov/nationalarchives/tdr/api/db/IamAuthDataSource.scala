package uk.gov.nationalarchives.tdr.api.db

import java.io.PrintWriter
import java.sql.{Connection, SQLException, SQLFeatureNotSupportedException}
import java.util.Properties
import java.util.logging.Logger
import javax.sql.DataSource
import scala.util.{Failure, Success, Try}

/** A data source which generates a new password every time the connection pool creates a physical connection. This means connections are always created with a valid IAM
  * authentication token, even when the pool replaces connections while the API is idle.
  */
class IamAuthDataSource(
    jdbcUrl: String,
    user: String,
    password: () => Try[String] = () => DatabasePassword.getPassword,
    connect: (String, Properties) => Connection = IamAuthDataSource.driverConnect
) extends DataSource {

  override def getConnection: Connection = password() match {
    case Success(currentPassword) => getConnection(user, currentPassword)
    case Failure(exception)       => throw new SQLException("Failed to generate a database authentication token", exception)
  }

  override def getConnection(username: String, currentPassword: String): Connection = {
    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password", currentPassword)
    connect(jdbcUrl, properties)
  }

  private var logWriter: Option[PrintWriter] = None
  private var loginTimeout: Int = 0

  override def getLogWriter: PrintWriter = logWriter.orNull

  override def setLogWriter(out: PrintWriter): Unit = logWriter = Option(out)

  override def setLoginTimeout(seconds: Int): Unit = loginTimeout = seconds

  override def getLoginTimeout: Int = loginTimeout

  override def getParentLogger: Logger = throw new SQLFeatureNotSupportedException()

  override def unwrap[T](iface: Class[T]): T =
    if (iface.isInstance(this)) iface.cast(this) else throw new SQLException(s"Cannot unwrap to ${iface.getName}")

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}

object IamAuthDataSource {
  private lazy val driver = new org.postgresql.Driver()

  def driverConnect(jdbcUrl: String, properties: Properties): Connection =
    Option(driver.connect(jdbcUrl, properties)).getOrElse(throw new SQLException(s"The postgresql driver does not accept the url $jdbcUrl"))
}
