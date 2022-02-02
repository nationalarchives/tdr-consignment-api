package uk.gov.nationalarchives.tdr.api.utils

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import java.util.UUID
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshaller}
import akka.stream.Materializer
import com.tngtech.keycloakmock.api.KeycloakVerificationMock
import com.tngtech.keycloakmock.api.TokenConfig.aTokenConfig
import io.circe.Decoder
import io.circe.parser.decode
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService._

import scala.concurrent.ExecutionContext
import scala.io.Source.fromResource

//scalastyle:off number.of.methods
object TestUtils {
  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")

  private val tdrPort: Int = 8000
  private val testPort: Int = 8001
  private val tdrMock: KeycloakVerificationMock = createServer("tdr", tdrPort)
  private val testMock: KeycloakVerificationMock = createServer("test", testPort)

  private def createServer(realm: String, port: Int): KeycloakVerificationMock = {
    val mock: KeycloakVerificationMock = new KeycloakVerificationMock(port, "tdr")
    mock.start()
    mock
  }

  val userId: UUID = UUID.fromString("4ab14990-ed63-4615-8336-56fbb9960300")
  val backendChecksUser: UUID = UUID.fromString("6847253d-b9c6-4ea9-b3c9-57542b8c6375")
  val reportingUser: UUID = UUID.fromString("a863292b-888b-4d88-b5f3-2bb9a11b336a")

  def validUserToken(userId: UUID = userId, body: String = "Code", standardUser: String = "true"): OAuth2BearerToken =
    OAuth2BearerToken(tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr", "tdr_user")
        .withClaim("body", body)
        .withClaim("user_id", userId)
        .withClaim("standard_user", standardUser)
        .build)
  )

  def validJudgmentUserToken(userId: UUID = userId, body: String = "Code", judgmentUser: String = "true"): OAuth2BearerToken =
    OAuth2BearerToken(tdrMock.getAccessToken(
      aTokenConfig()
        .withResourceRole("tdr", "tdr_user")
        .withClaim("body", body)
        .withClaim("user_id", userId)
        .withClaim("judgment_user", judgmentUser)
        .build
    ))

  def validUserTokenNoBody: OAuth2BearerToken = OAuth2BearerToken(tdrMock.getAccessToken(
    aTokenConfig()
      .withResourceRole("tdr", "tdr_user")
      .withClaim("user_id", userId)
      .build)
  )

  def validBackendChecksToken(role: String): OAuth2BearerToken = OAuth2BearerToken(tdrMock.getAccessToken(
    aTokenConfig()
      .withResourceRole("tdr-backend-checks", role)
      .withClaim("user_id", backendChecksUser)
      .build
  ))

  def invalidBackendChecksToken(): OAuth2BearerToken = OAuth2BearerToken(tdrMock.getAccessToken(
    aTokenConfig()
      .withClaim("user_id", backendChecksUser)
      .withResourceRole("tdr-backend-checks", "some_role").build
  ))

  def validReportingToken(role: String): OAuth2BearerToken = OAuth2BearerToken(tdrMock.getAccessToken(
    aTokenConfig()
      .withResourceRole("tdr-reporting", role)
      .withClaim("user_id", reportingUser)
      .build
  ))

  def invalidReportingToken(): OAuth2BearerToken = OAuth2BearerToken(tdrMock.getAccessToken(
    aTokenConfig()
      .withClaim("user_id", reportingUser)
      .withResourceRole("tdr-reporting", "some_role")
      .build
  ))

  def invalidToken: OAuth2BearerToken = OAuth2BearerToken(testMock.getAccessToken(aTokenConfig().build))

  case class GraphqlError(message: String, extensions: Option[GraphqlErrorExtensions])

  case class GraphqlErrorExtensions(code: String)

  case class Locations(column: Int, line: Int)

  def getDataFromFile[A](prefix: String)(fileName: String)(implicit decoder: Decoder[A]): A = {
    getDataFromString(fromResource(s"$prefix$fileName.json").mkString)
  }

  def getDataFromString[A](dataString: String)(implicit decoder: Decoder[A]): A = {
    val result: Either[io.circe.Error, A] = decode[A](dataString)
    result match {
      case Right(data) => data
      case Left(e) => throw e
    }
  }

  def unmarshalResponse[A]()(implicit mat: Materializer, ec: ExecutionContext, decoder: Decoder[A]): FromResponseUnmarshaller[A] = Unmarshaller(_ => {
    res => {
      Unmarshaller.stringUnmarshaller(res.entity).map(s => getDataFromString[A](s))
    }
  })

  def seedDatabaseWithDefaultEntries(consignmentType: String = "standard"): Unit = {
    val consignmentId = UUID.fromString("eb197bfb-43f7-40ca-9104-8f6cbda88506")
    val seriesId = UUID.fromString("1436ad43-73a2-4489-a774-85fa95daff32")
    createConsignment(consignmentId, userId, seriesId, consignmentType = consignmentType)
    createFile(defaultFileId, consignmentId)
    createClientFileMetadata(defaultFileId)
  }

  //scalastyle:off magic.number
  def createConsignment(
                         consignmentId: UUID,
                         userId: UUID,
                         seriesId: UUID = UUID.fromString("9e2e2a51-c2d0-4b99-8bef-2ca322528861"),
                         consignmentRef: String = "TDR-2021-TESTMTB",
                         consignmentType: String = "standard",
                         bodyId: UUID = UUID.fromString("4da472a5-16b3-4521-a630-5917a0722359")): Unit = {
    val sql = "INSERT INTO Consignment" +
      "(ConsignmentId, SeriesId, UserId, Datetime, TransferInitiatedDatetime, ExportDatetime, ConsignmentReference, ConsignmentType, BodyId)" +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    val fixedTimeStamp = Timestamp.from(FixedTimeSource.now)
    ps.setString(1, consignmentId.toString)
    ps.setString(2, seriesId.toString)
    ps.setString(3, userId.toString)
    ps.setTimestamp(4, fixedTimeStamp)
    ps.setTimestamp(5, fixedTimeStamp)
    ps.setTimestamp(6, fixedTimeStamp)
    ps.setString(7, consignmentRef)
    ps.setString(8, consignmentType)
    ps.setString(9, bodyId.toString)
    ps.executeUpdate()
  }
  //scalastyle:on magic.number

  def getConsignment(consignmentId: UUID): ResultSet = {
    val sql = s"SELECT * FROM Consignment WHERE ConsignmentId = ?"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, consignmentId.toString)
    val result = ps.executeQuery()
    result.next()
    result
  }

  //scalastyle:off magic.number
  def createConsignmentStatus(consignmentId: UUID,
                                    statusType: String,
                                    statusValue: String,
                                    createdDate: Timestamp = Timestamp.from(FixedTimeSource.now)
                                   ): Unit = {
    val sql =
      s"INSERT INTO ConsignmentStatus (ConsignmentStatusId, ConsignmentId, StatusType, Value, CreatedDatetime) VALUES (?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, UUID.randomUUID().toString)
    ps.setString(2, consignmentId.toString)
    ps.setString(3, statusType)
    ps.setString(4, statusValue)
    ps.setTimestamp(5, createdDate)
    ps.executeUpdate()
  }

  def getConsignmentStatus(consignmentId: UUID, statusType: String): ResultSet = {
    val sql = "SELECT Value FROM ConsignmentStatus WHERE ConsignmentId = ? AND StatusType = ?"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, consignmentId.toString)
    ps.setString(2, statusType)
    val result = ps.executeQuery()
    result.next()
    result
  }

  def createFile(fileId: UUID, consignmentId: UUID, fileType: String = NodeType.fileTypeIdentifier, parentId: Option[UUID] = None): Unit = {
    val sql = s"INSERT INTO File (FileId, ConsignmentId, UserId, Datetime, FileType, ParentId) VALUES (?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId.toString)
    ps.setString(2, consignmentId.toString)
    ps.setString(3, userId.toString)
    ps.setTimestamp(4, Timestamp.from(FixedTimeSource.now))
    ps.setString(5, fileType)
    ps.setString(6, parentId.map(_.toString).orNull)
    ps.executeUpdate()
  }

  //scalastyle:off magic.number
  def addAntivirusMetadata(fileId: String, result: String = "Result of AVMetadata processing"): Unit = {
    val sql = s"INSERT INTO AVMetadata (FileId, Software, SoftwareVersion, DatabaseVersion, Result, Datetime) VALUES (?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId)
    ps.setString(2, "Some antivirus software")
    ps.setString(3, "Some software version")
    ps.setString(4, "Some database version")
    ps.setString(5, result)
    ps.setTimestamp(6, Timestamp.from(FixedTimeSource.now))
    ps.executeUpdate()
  }

  def addFileMetadata(metadataId: String, fileId: String, propertyName: String, value: String = "Result of FileMetadata processing"): Unit = {
    val sql = s"INSERT INTO FileMetadata (MetadataId, FileId, Value, Datetime, UserId, PropertyName) VALUES (?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, metadataId)
    ps.setString(2, fileId)
    ps.setString(3, value)
    ps.setTimestamp(4, Timestamp.from(FixedTimeSource.now))
    ps.setString(5, userId.toString)
    ps.setString(6, propertyName)

    ps.executeUpdate()
  }

  def addFFIDMetadata(fileId: String,
                      software: String="TEST DATA software",
                      softwareVersion: String="TEST DATA software version",
                      binarySigFileVersion: String="TEST DATA binary signature file version",
                      containerSigFileVersion: String="TEST DATA container signature file version",
                      method: String="TEST DATA method"
                     ): UUID = {
    val ffidMetadataId = java.util.UUID.randomUUID()
    val sql = s"INSERT INTO FFIDMetadata" +
      s"(FileId, Software, SoftwareVersion, BinarySignatureFileVersion, ContainerSignatureFileVersion, Method, Datetime, FFIDMetadataId)" +
      s"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, fileId)
    ps.setString(2, software)
    ps.setString(3, softwareVersion)
    ps.setString(4, binarySigFileVersion)
    ps.setString(5, containerSigFileVersion)
    ps.setString(6, method)
    ps.setTimestamp(7, Timestamp.from(FixedTimeSource.now))
    ps.setObject(8, ffidMetadataId)

    ps.executeUpdate()
    ffidMetadataId // ffidMetadataId has to be returned so that the addFFIDMetadataMatches method can be called with it
  }

  def addFFIDMetadataMatches(ffidMetadataId: String,
                             extension: String="txt",
                             identificationBasis: String="TEST DATA identification",
                             puid: String="TEST DATA puid"
                            ): Unit = {
    val sql = s"INSERT INTO FFIDMetadataMatches" +
      s"(FFIDMetadataId, Extension, IdentificationBasis, PUID)" +
      s"VALUES (?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, ffidMetadataId)
    ps.setString(2, extension)
    ps.setString(3, identificationBasis)
    ps.setString(4, puid)

    ps.executeUpdate()
  }

  def createClientFileMetadata(fileId: UUID): Unit = {
    val sql = "INSERT INTO FileMetadata(MetadataId, FileId, Value, Datetime, UserId, PropertyName) VALUES (?,?,?,?,?,?)"
    clientSideProperties.foreach(propertyName => {
      val value = propertyName match {
        case ClientSideFileLastModifiedDate => Timestamp.from(Instant.now()).toString
        case ClientSideFileSize => "1"
        case SHA256ClientSideChecksum => "ddb1584d8cb5f07dc6602f58bd5e0184c87e223787af7b619ce04319727b83bf"
        case _ => s"$propertyName Value"
      }
      val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
      ps.setString(1, UUID.randomUUID().toString)
      ps.setString(2, fileId.toString)
      ps.setString(3, value)
      ps.setTimestamp(4, Timestamp.from(Instant.now()))
      ps.setString(5, UUID.randomUUID().toString)
      ps.setString(6, propertyName)
      ps.executeUpdate()
    })

  }

  //scalastyle:on magic.number

  def addFileProperty(name: String): Unit = {
    val sql = s"INSERT INTO FileProperty (Name) VALUES (?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, name)

    ps.executeUpdate()
  }

  def addParentFolderName(consignmentId: UUID, parentFolderName: String): Unit = {
    val sql = s"update Consignment set ParentFolder=\'$parentFolderName\' where ConsignmentId=\'$consignmentId\'"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)

    ps.executeUpdate()
  }

  def addTransferringBody(id: UUID, name: String, code: String): Unit = {
    val sql = s"INSERT INTO Body (BodyId, Name, TdrCode) VALUES (?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)

    ps.setString(1, id.toString)
    ps.setString(2, name)
    ps.setString(3, code)

    ps.executeUpdate()
  }

  // scalastyle:off magic.number
  def addSeries(
                 seriesId: UUID,
                 bodyId: UUID,
                 code: String,
                 name: String = "some-series-name",
                 description: String = "some-series-description"
               ): Unit = {
    val sql = s"INSERT INTO Series (SeriesId, BodyId, Code, Name, Description) VALUES (?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, seriesId.toString)
    ps.setString(2, bodyId.toString)
    ps.setString(3, code)
    ps.setString(4, name)
    ps.setString(5, description)

    ps.executeUpdate()
  }
  // scalastyle:on magic.number

  def addConsignmentProperty(name: String): Unit = {
    // name is primary key check exists before attempting insert to table
    if (!propertyExists(name)) {
      val sql = s"INSERT INTO ConsignmentProperty (Name) VALUES (?)"
      val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
      ps.setString(1, name)
      ps.executeUpdate()
    }
  }

  private def propertyExists(name: String): Boolean = {
    val sql = s"SELECT * FROM ConsignmentProperty WHERE Name = ?"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, name)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
  }

  //scalastyle:off magic.number
  def addConsignmentMetadata(metadataId: String, consignmentId: String, propertyName: String): Unit = {
    val sql = s"insert into ConsignmentMetadata (MetadataId, ConsignmentId, PropertyName, Value, Datetime, UserId) VALUES (?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
    ps.setString(1, metadataId)
    ps.setString(2, consignmentId)
    ps.setString(3, propertyName)
    ps.setString(4, "Result of ConsignmentMetadata processing")
    ps.setTimestamp(5, Timestamp.from(FixedTimeSource.now))
    ps.setString(6, userId.toString)

    ps.executeUpdate()
  }

  def addTransferAgreementMetadata(consignmentId: UUID): Unit = {
    val sql = "INSERT INTO ConsignmentMetadata(MetadataId, ConsignmentId, PropertyName, Value, Datetime, UserId) VALUES (?,?,?,?,?,?)"
    transferAgreementProperties.foreach(propertyName => {
      val ps: PreparedStatement = DbConnection.db.source.createConnection().prepareStatement(sql)
      ps.setString(1, UUID.randomUUID().toString)
      ps.setString(2, consignmentId.toString)
      ps.setString(3, propertyName)
      ps.setString(4, true.toString)
      ps.setTimestamp(5, Timestamp.from(Instant.now()))
      ps.setString(6, UUID.randomUUID().toString)
      ps.executeUpdate()
    })
  }
  //scalastyle:on magic.number
}
