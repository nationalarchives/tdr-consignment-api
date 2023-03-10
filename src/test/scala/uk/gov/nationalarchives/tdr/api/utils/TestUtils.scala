package uk.gov.nationalarchives.tdr.api.utils

import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshaller}
import akka.stream.Materializer
import io.circe.Decoder
import io.circe.parser.decode
import slick.jdbc.JdbcBackend
import uk.gov.nationalarchives.tdr.api.model.Statuses.{ClosureMetadataType, DescriptiveMetadataType, NotEnteredValue, PasswordProtectedValue, ZipValue}
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService._
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService._
import uk.gov.nationalarchives.tdr.api.utils.TestAuthUtils.userId
import uk.gov.nationalarchives.tdr.api.utils.TestUtils._

import java.sql._
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.io.Source.fromResource

//scalastyle:off number.of.methods
class TestUtils(db: JdbcBackend#DatabaseDef) {
  val connection: Connection = db.source.createConnection()

  def deleteTables(): Boolean = {
    connection.prepareStatement("""DELETE FROM "FileStatus";""").execute()
    connection.prepareStatement("""DELETE FROM "FileMetadata";""").execute()
    connection.prepareStatement("""DELETE FROM "FileProperty";""").execute()
    connection.prepareStatement("""DELETE FROM "FFIDMetadataMatches";""").execute()
    connection.prepareStatement("""DELETE FROM "FFIDMetadata";""").execute()
    connection.prepareStatement("""DELETE FROM "AVMetadata";""").execute()
    connection.prepareStatement("""DELETE FROM "File";""").execute()
    connection.prepareStatement("""DELETE FROM "ConsignmentMetadata";""").execute()
    connection.prepareStatement("""DELETE FROM "ConsignmentProperty";""").execute()
    connection.prepareStatement("""DELETE FROM "ConsignmentStatus";""").execute()
    connection.prepareStatement("""DELETE FROM "Consignment";""").execute()
    connection.prepareStatement("""DELETE FROM "DisallowedPuids";""").execute()
    connection.prepareStatement("""DELETE FROM "AllowedPuids";""").execute()
    connection.prepareStatement("""ALTER SEQUENCE consignment_sequence_id RESTART WITH 1;""").execute()
  }

  def deleteSeriesAndBody(): Int = {
    connection.prepareStatement("""DELETE FROM "Series"; """).executeUpdate()
    connection.prepareStatement("""DELETE FROM "Body"; """).executeUpdate()
  }

  def addTransferAgreementConsignmentProperties(): Unit = {
    transferAgreementProperties.foreach(propertyName => {
      addConsignmentProperty(propertyName)
    })
  }

  def addFinalTransferConfirmationProperties(): Unit = {
    finalTransferConfirmationProperties.foreach(propertyName => {
      addConsignmentProperty(propertyName)
    })
  }

  def addFinalJudgmentTransferConfirmationProperties(): Unit = {
    finalJudgmentTransferConfirmationProperties.foreach(propertyName => {
      addConsignmentProperty(propertyName)
    })
  }

  def addClientSideProperties(): Unit = {
    clientSideProperties.foreach(propertyName => {
      addFileProperty(propertyName)
    })
  }

  def countAllFileMetadata(): Int = {
    val sql = s"""SELECT COUNT(*) as num FROM "FileMetadata";"""
    val ps = connection.prepareStatement(sql)
    val rs = ps.executeQuery()
    rs.next()
    rs.getInt("num")
  }

  def countFileMetadata(fileId: UUID): Int = {
    val sql = s"""SELECT COUNT(*) as num FROM "FileMetadata" WHERE "FileId" = ? AND "PropertyName" = ?;"""
    val ps = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, "FileProperty")
    val rs = ps.executeQuery()
    rs.next()
    rs.getInt("num")
  }

  def createFileStatusValues(fileStatusId: UUID, FileId: UUID, statusType: String, value: String): Unit = {
    val sql = s"""INSERT INTO "FileStatus" ("FileStatusId", "FileId", "StatusType", "Value", "CreatedDatetime") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileStatusId, Types.OTHER)
    ps.setObject(2, FileId, Types.OTHER)
    ps.setString(3, statusType)
    ps.setString(4, value)
    ps.setTimestamp(5, Timestamp.from(Instant.now()))
    ps.executeUpdate()
  }

  def getFileStatusResult(fileId: UUID, statusType: String): List[String] = {
    val sql = s"""SELECT "Value" FROM "FileStatus" where "FileId" = ? AND "StatusType" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, statusType)
    val rs: ResultSet = ps.executeQuery()

    new Iterator[String] {
      def hasNext: Boolean = rs.next()

      def next(): String = rs.getString(1)
    }.to(LazyList).toList
  }

  def createFilePropertyValues(propertyName: String, propertyValue: String, default: Boolean, dependencies: Int, secondaryvalue: Int, uiOrdinal: Option[Int] = None): Unit = {
    val sql = s"""INSERT INTO "FilePropertyValues" ("PropertyName", "PropertyValue", "Default", "Dependencies", "SecondaryValue", "Ordinal") VALUES (?, ?, ?, ?, ?, ?)"""

    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, propertyName)
    ps.setString(2, propertyValue)
    ps.setBoolean(3, default)
    ps.setInt(4, dependencies)
    ps.setInt(5, secondaryvalue)
    ps.setInt(6, uiOrdinal.getOrElse(Int.MinValue))
    ps.executeUpdate()
  }

  def createFilePropertyDependencies(groupId: Int, propertyName: String, default: String): Unit = {
    val sql = s"""INSERT INTO "FilePropertyDependencies" ("GroupId", "PropertyName", "Default") VALUES (?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setInt(1, groupId)
    ps.setString(2, propertyName)
    ps.setString(3, default)
    ps.executeUpdate()
  }

  def createAllowedPuids(puid: String, description: String, consignmentType: String): Unit = {
    val sql = s"""INSERT INTO "AllowedPuids" ("PUID", "PUID Description", "Created Date", "Modified Date", "ConsignmentType") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, puid)
    ps.setString(2, description)
    ps.setTimestamp(3, Timestamp.from(Instant.now()))
    ps.setTimestamp(4, Timestamp.from(Instant.now()))
    ps.setString(5, consignmentType)
    ps.executeUpdate()
  }

  def createDisallowedPuids(puid: String, description: String, reason: String, active: Boolean = true): Unit = {
    val sql = s"""INSERT INTO "DisallowedPuids" ("PUID", "PUID Description", "Created Date", "Modified Date", "Reason", "Active") VALUES (?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, puid)
    ps.setString(2, description)
    ps.setTimestamp(3, Timestamp.from(Instant.now()))
    ps.setTimestamp(4, Timestamp.from(Instant.now()))
    ps.setString(5, reason)
    ps.setBoolean(6, active)
    ps.executeUpdate()
  }

  def createFileProperty(
      name: String,
      description: String,
      propertytype: String,
      datatype: String,
      editable: Boolean,
      multivalue: Boolean,
      propertygroup: String,
      fullname: String,
      exportOrdinal: Int = 1,
      allowExport: Boolean = false
  ): Unit = {
    val sql =
      s"""INSERT INTO "FileProperty" ("Name", "Description", "CreatedDatetime", "ModifiedDatetime",""" +
        s""" "PropertyType", "Datatype", "Editable", "MultiValue", "PropertyGroup", "FullName", "ExportOrdinal", "AllowExport") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)
    ps.setString(2, description)
    ps.setTimestamp(3, Timestamp.from(Instant.now()))
    ps.setTimestamp(4, Timestamp.from(Instant.now()))
    ps.setString(5, propertytype)
    ps.setString(6, datatype)
    ps.setBoolean(7, editable)
    ps.setBoolean(8, multivalue)
    ps.setString(9, propertygroup)
    ps.setString(10, fullname)
    ps.setInt(11, exportOrdinal)
    ps.setBoolean(12, allowExport)
    ps.executeUpdate()
  }

  def seedDatabaseWithDefaultEntries(consignmentType: String = "standard"): (UUID, UUID) = {
    val consignmentId = UUID.fromString("eb197bfb-43f7-40ca-9104-8f6cbda88506")
    createConsignment(consignmentId, userId, fixedSeriesId, consignmentType = consignmentType)
    createFile(defaultFileId, consignmentId)

    createClientFileMetadata(defaultFileId)

    createDisallowedPuids("fmt/289", "WARC", ZipValue.value)
    createDisallowedPuids("fmt/329", "Shell Archive Format", ZipValue.value)
    createDisallowedPuids("fmt/754", "Microsoft Word Document", PasswordProtectedValue.value)
    createDisallowedPuids("fmt/494", "Microsoft Office Encrypted Document", PasswordProtectedValue.value)

    createAllowedPuids("fmt/412", "Microsoft Word for Windows", "judgment")

    (consignmentId, defaultFileId)
  }

  def createConsignment(
      consignmentId: UUID,
      userId: UUID = userId,
      seriesId: UUID = fixedSeriesId,
      consignmentRef: String = s"TDR-${Instant.now.getNano}-TESTMTB",
      consignmentType: String = "standard",
      bodyId: UUID = fixedBodyId,
      includeStatusRows: Boolean = true
  ): UUID = {
    val sql =
      """INSERT INTO "Consignment" """ +
        """("ConsignmentId", "SeriesId", "UserId", "Datetime", "TransferInitiatedDatetime",
          |"ExportDatetime", "ConsignmentReference", "ConsignmentType", "BodyId", "ConsignmentSequence")""".stripMargin +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val nextValStatement = connection.prepareStatement("select nextval('consignment_sequence_id') as Seq")
    val nextResults: ResultSet = nextValStatement.executeQuery()
    nextResults.next()
    val nextSequence: Int = nextResults.getInt("Seq") + 1

    val ps: PreparedStatement = connection.prepareStatement(sql)
    val fixedTimeStamp = Timestamp.from(FixedTimeSource.now)
    ps.setObject(1, consignmentId, Types.OTHER)
    ps.setObject(2, seriesId, Types.OTHER)
    ps.setObject(3, userId, Types.OTHER)
    ps.setTimestamp(4, fixedTimeStamp)
    ps.setTimestamp(5, fixedTimeStamp)
    ps.setTimestamp(6, fixedTimeStamp)
    ps.setString(7, consignmentRef)
    ps.setString(8, consignmentType)
    ps.setObject(9, bodyId, Types.OTHER)
    ps.setInt(10, nextSequence)
    ps.executeUpdate()
    if (includeStatusRows) {
      createConsignmentStatus(consignmentId, DescriptiveMetadataType.id, NotEnteredValue.value)
      createConsignmentStatus(consignmentId, ClosureMetadataType.id, NotEnteredValue.value)
    }
    consignmentId
  }

  def getConsignment(consignmentId: UUID): ResultSet = {
    val sql = s"""SELECT * FROM "Consignment" WHERE "ConsignmentId" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    val result = ps.executeQuery()
    result.next()
    result
  }

  def createConsignmentStatus(
      consignmentId: UUID,
      statusType: String,
      statusValue: String,
      createdDate: Timestamp = Timestamp.from(FixedTimeSource.now),
      statusId: UUID = UUID.randomUUID()
  ): Unit = {
    val sql =
      s"""INSERT INTO "ConsignmentStatus" ("ConsignmentStatusId", "ConsignmentId", "StatusType", "Value", "CreatedDatetime") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, statusId, Types.OTHER)
    ps.setObject(2, consignmentId, Types.OTHER)
    ps.setString(3, statusType)
    ps.setString(4, statusValue)
    ps.setTimestamp(5, createdDate)
    ps.executeUpdate()
  }

  def getConsignmentStatus(consignmentId: UUID, statusType: String): ResultSet = {
    val sql = """SELECT "ConsignmentId", "Value" FROM "ConsignmentStatus" WHERE "ConsignmentId" = ? AND "StatusType" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    ps.setString(2, statusType)
    val result = ps.executeQuery()
    result.next()
    result
  }

  def createFile(
      fileId: UUID,
      consignmentId: UUID,
      fileType: String = NodeType.fileTypeIdentifier,
      fileName: String = "fileName",
      parentId: Option[UUID] = None,
      userId: UUID = userId
  ): Unit = {
    val sql = s"""INSERT INTO "File" ("FileId", "ConsignmentId", "UserId", "Datetime", "FileType", "FileName", "ParentId") VALUES (?, ?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setObject(2, consignmentId, Types.OTHER)
    ps.setObject(3, userId, Types.OTHER)
    ps.setTimestamp(4, Timestamp.from(FixedTimeSource.now))
    ps.setString(5, fileType)
    ps.setString(6, parentId.toString)
    ps.setString(6, fileName)
    ps.setObject(7, parentId.map(_.toString).orNull, Types.OTHER)
    ps.executeUpdate()
  }

  def getAntivirusMetadata(fileId: Option[UUID] = None): ResultSet = {
    val ps = fileId
      .map(id => {
        val ps = connection.prepareStatement("""SELECT * FROM "AVMetadata" WHERE "FileId" = ?; """)
        ps.setObject(1, id, Types.OTHER)
        ps
      })
      .getOrElse(connection.prepareStatement("""SELECT * FROM "AVMetadata";"""))
    ps.executeQuery()
  }

  def addAntivirusMetadata(fileId: String, result: String = "Result of AVMetadata processing"): Unit = {
    val sql = s"""INSERT INTO "AVMetadata" ("FileId", "Software", "SoftwareVersion", "DatabaseVersion", "Result", "Datetime") VALUES (?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, "Some antivirus software")
    ps.setString(3, "Some software version")
    ps.setString(4, "Some database version")
    ps.setString(5, result)
    ps.setTimestamp(6, Timestamp.from(FixedTimeSource.now))
    ps.executeUpdate()
  }

  def addFileMetadata(metadataId: String, fileId: String, propertyName: String, value: String = "Result of FileMetadata processing"): Unit = {
    val sql = s"""INSERT INTO "FileMetadata" ("MetadataId", "FileId", "Value", "Datetime", "UserId", "PropertyName") VALUES (?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, metadataId, Types.OTHER)
    ps.setObject(2, fileId, Types.OTHER)
    ps.setString(3, value)
    ps.setTimestamp(4, Timestamp.from(FixedTimeSource.now))
    ps.setObject(5, userId, Types.OTHER)
    ps.setString(6, propertyName)

    ps.executeUpdate()
  }

  def addFFIDMetadata(
      fileId: String,
      software: String = "TEST DATA software",
      softwareVersion: String = "TEST DATA software version",
      binarySigFileVersion: String = "TEST DATA binary signature file version",
      containerSigFileVersion: String = "TEST DATA container signature file version",
      method: String = "TEST DATA method"
  ): UUID = {
    val ffidMetadataId = java.util.UUID.randomUUID()
    val sql =
      s"""INSERT INTO "FFIDMetadata" """ +
        s"""("FileId", "Software", "SoftwareVersion", "BinarySignatureFileVersion",
           |"ContainerSignatureFileVersion", "Method", "Datetime", "FFIDMetadataId")""".stripMargin +
        s"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setString(2, software)
    ps.setString(3, softwareVersion)
    ps.setString(4, binarySigFileVersion)
    ps.setString(5, containerSigFileVersion)
    ps.setString(6, method)
    ps.setTimestamp(7, Timestamp.from(FixedTimeSource.now))
    ps.setObject(8, ffidMetadataId, Types.OTHER)

    ps.executeUpdate()
    ffidMetadataId // ffidMetadataId has to be returned so that the addFFIDMetadataMatches method can be called with it
  }

  def addFFIDMetadataMatches(ffidMetadataId: String, extension: String = "txt", identificationBasis: String = "TEST DATA identification", puid: String = "TEST DATA puid"): Unit = {
    val sql =
      s"""INSERT INTO "FFIDMetadataMatches" """ +
        s"""("FFIDMetadataId", "Extension", "IdentificationBasis", "PUID")""" +
        s"VALUES (?, ?, ?, ?)"
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, ffidMetadataId, Types.OTHER)
    ps.setString(2, extension)
    ps.setString(3, identificationBasis)
    ps.setString(4, puid)

    ps.executeUpdate()
  }

  def createClientFileMetadata(fileId: UUID): Unit = {
    val sql = """INSERT INTO "FileMetadata"("MetadataId", "FileId", "Value", "Datetime", "UserId", "PropertyName") VALUES (?,?,?,?,?,?)"""

    clientSideProperties.foreach(propertyName => {
      addFileProperty(propertyName)
      val value = propertyName match {
        case ClientSideFileLastModifiedDate => Timestamp.from(Instant.now()).toString
        case ClientSideFileSize             => "1"
        case SHA256ClientSideChecksum       => "ddb1584d8cb5f07dc6602f58bd5e0184c87e223787af7b619ce04319727b83bf"
        case _                              => s"$propertyName Value"
      }
      val ps: PreparedStatement = connection.prepareStatement(sql)
      ps.setObject(1, UUID.randomUUID(), Types.OTHER)
      ps.setObject(2, fileId, Types.OTHER)
      ps.setString(3, value)
      ps.setTimestamp(4, Timestamp.from(Instant.now()))
      ps.setObject(5, UUID.randomUUID(), Types.OTHER)
      ps.setString(6, propertyName)
      ps.executeUpdate()
    })
  }

  def addFileProperty(name: String, propertyGroup: String): Unit = {
    val sql = s"""INSERT INTO "FileProperty" ("Name", "PropertyType", "Datatype", "PropertyGroup") VALUES (?, ?, ?, ?)"""
    val defaultPropertyType = "System"
    val defaultDataType = "text"
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)
    ps.setString(2, defaultPropertyType)
    ps.setString(3, defaultDataType)
    ps.setString(4, propertyGroup)
    ps.executeUpdate()
  }

  def addFileProperty(name: String): Unit = {
    val defaultPropertyType = "System"
    val defaultDataType = "text"
    val sql = s"""INSERT INTO "FileProperty" ("Name", "PropertyType", "Datatype") VALUES (?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)
    ps.setString(2, defaultPropertyType)
    ps.setString(3, defaultDataType)

    ps.executeUpdate()
  }

  def addParentFolderName(consignmentId: UUID, parentFolderName: String): Unit = {
    val sql = s"""update "Consignment" set "ParentFolder"='$parentFolderName' where "ConsignmentId"='$consignmentId'"""
    val ps: PreparedStatement = connection.prepareStatement(sql)

    ps.executeUpdate()
  }

  def addTransferringBody(id: UUID, name: String, code: String): Unit = {
    val sql = s"""INSERT INTO "Body" ("BodyId", "Name", "TdrCode") VALUES (?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)

    ps.setObject(1, id, Types.OTHER)
    ps.setString(2, name)
    ps.setString(3, code)

    ps.executeUpdate()
  }

  def addSeries(
      seriesId: UUID,
      bodyId: UUID,
      code: String,
      name: String = "some-series-name",
      description: String = "some-series-description"
  ): Unit = {
    val sql = s"""INSERT INTO "Series" ("SeriesId", "BodyId", "Code", "Name", "Description") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, seriesId, Types.OTHER)
    ps.setObject(2, bodyId, Types.OTHER)
    ps.setString(3, code)
    ps.setString(4, name)
    ps.setString(5, description)

    ps.executeUpdate()
  }

  def addConsignmentProperty(name: String): Unit = {
    // name is primary key check exists before attempting insert to table
    if (!propertyExists(name)) {
      val sql = s"""INSERT INTO "ConsignmentProperty" ("Name") VALUES (?)"""
      val ps: PreparedStatement = connection.prepareStatement(sql)
      ps.setString(1, name)
      ps.executeUpdate()
    }
  }

  private def propertyExists(name: String): Boolean = {
    val sql = s"""SELECT * FROM "ConsignmentProperty" WHERE "Name" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)
    val rs: ResultSet = ps.executeQuery()
    rs.next()
  }

  def addConsignmentMetadata(metadataId: UUID, consignmentId: UUID, propertyName: String): Unit = {
    val sql = s"""insert into "ConsignmentMetadata" ("MetadataId", "ConsignmentId", "PropertyName", "Value", "Datetime", "UserId") VALUES (?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, metadataId, Types.OTHER)
    ps.setObject(2, consignmentId, Types.OTHER)
    ps.setString(3, propertyName)
    ps.setString(4, "Result of ConsignmentMetadata processing")
    ps.setTimestamp(5, Timestamp.from(FixedTimeSource.now))
    ps.setObject(6, userId, Types.OTHER)

    ps.executeUpdate()
  }

  def addTransferAgreementMetadata(consignmentId: UUID): Unit = {
    val sql = """INSERT INTO "ConsignmentMetadata" ("MetadataId", "ConsignmentId", "PropertyName", "Value", "Datetime", "UserId") VALUES (?,?,?,?,?,?)"""
    transferAgreementProperties.foreach(propertyName => {
      val ps: PreparedStatement = connection.prepareStatement(sql)
      ps.setObject(1, UUID.randomUUID(), Types.OTHER)
      ps.setString(2, consignmentId.toString)
      ps.setString(3, propertyName)
      ps.setString(4, true.toString)
      ps.setTimestamp(5, Timestamp.from(Instant.now()))
      ps.setObject(6, UUID.randomUUID(), Types.OTHER)
      ps.executeUpdate()
    })
  }

  def resetConsignmentSequence(): Unit = {
    val sql = """ALTER SEQUENCE "consignment_sequence_id" RESTART;"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.executeUpdate()
  }

  def createDisplayProperty(propertyName: String, attribute: String, value: String, attributeType: String): Unit = {
    val sql = s"""INSERT INTO "DisplayProperties" ("PropertyName", "Attribute", "Value", "AttributeType") VALUES (?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, propertyName)
    ps.setString(2, attribute)
    ps.setString(3, value)
    ps.setString(4, attributeType)
    ps.executeUpdate()
  }
}

object TestUtils {
  val fixedSeriesId: UUID = UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e")
  val fixedBodyId: UUID = UUID.fromString("4da472a5-16b3-4521-a630-5917a0722359")

  def apply(db: JdbcBackend#DatabaseDef): TestUtils = new TestUtils(db)

  def getDataFromFile[A](prefix: String)(fileName: String)(implicit decoder: Decoder[A]): A = {
    getDataFromString(fromResource(s"$prefix$fileName.json").mkString)
  }

  def getDataFromString[A](dataString: String)(implicit decoder: Decoder[A]): A = {
    val result: Either[io.circe.Error, A] = decode[A](dataString)
    result match {
      case Right(data) => data
      case Left(e)     => throw e
    }
  }

  def unmarshalResponse[A]()(implicit mat: Materializer, ec: ExecutionContext, decoder: Decoder[A]): FromResponseUnmarshaller[A] = Unmarshaller(_ => { res =>
    {
      Unmarshaller.stringUnmarshaller(res.entity).map(s => getDataFromString[A](s))
    }
  })

  case class GraphqlError(message: String, extensions: Option[GraphqlErrorExtensions])

  case class GraphqlErrorExtensions(code: String)

  case class Locations(column: Int, line: Int)

  val defaultFileId: UUID = UUID.fromString("07a3a4bd-0281-4a6d-a4c1-8fa3239e1313")
  val staticMetadataProperties: List[StaticMetadata] = List(RightsCopyright, LegalStatus, HeldBy, Language, FoiExemptionCode)
}
