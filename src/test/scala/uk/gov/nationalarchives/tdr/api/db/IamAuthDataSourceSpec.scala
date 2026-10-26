package uk.gov.nationalarchives.tdr.api.db

import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, SQLException}
import java.util.Properties
import scala.util.{Failure, Success}

class IamAuthDataSourceSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "getConnection" should "generate a new password for each connection the pool creates" in {
    val passwords = Iterator("token1", "token2")
    val passwordsUsed = scala.collection.mutable.ListBuffer[String]()
    val dataSource = new IamAuthDataSource(
      "jdbc:postgresql://localhost:5432/consignmentapi",
      "consignment_api_user",
      () => Success(passwords.next()),
      (_, properties) => {
        passwordsUsed += properties.getProperty("password")
        mock[Connection]
      }
    )

    dataSource.getConnection
    dataSource.getConnection

    passwordsUsed.toList should equal(List("token1", "token2"))
  }

  "getConnection" should "set the configured user on the connection properties" in {
    var propertiesUsed = new Properties()
    val dataSource = new IamAuthDataSource(
      "jdbc:postgresql://localhost:5432/consignmentapi",
      "consignment_api_user",
      () => Success("token"),
      (_, properties) => {
        propertiesUsed = properties
        mock[Connection]
      }
    )

    dataSource.getConnection

    propertiesUsed.getProperty("user") should equal("consignment_api_user")
  }

  "getConnection" should "throw an exception if the password cannot be generated" in {
    val cause = new RuntimeException("Token generation failed")
    val dataSource = new IamAuthDataSource(
      "jdbc:postgresql://localhost:5432/consignmentapi",
      "consignment_api_user",
      () => Failure[String](cause),
      (_, _) => mock[Connection]
    )

    val exception = the[SQLException] thrownBy dataSource.getConnection
    exception.getMessage should equal("Failed to generate a database authentication token")
    exception.getCause should equal(cause)
  }

}
