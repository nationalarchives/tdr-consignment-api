package uk.gov.nationalarchives.tdr.api.utils

import com.typesafe.config.Config
import uk.gov.nationalarchives.tdr.api.model.file.NodeType
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService._
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService.transferAgreementProperties
import uk.gov.nationalarchives.tdr.api.utils.TestContainerUtils.{fixedBodyId, fixedSeriesId}
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{defaultFileId, userId}

import java.sql._
import java.time.Instant
import java.util.UUID

class DatabaseUtils(connection: Connection) {
  def createDatabaseError(): Int = {
    val sql = """ALTER TABLE "Consignment" RENAME COLUMN "ConsignmentReference" TO "ConsignmentReferenceOld"; """
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.executeUpdate()
  }


  def deleteSeries(): Unit = {
    val sql = """DELETE FROM "Series" WHERE "SeriesId" = ? """
    val preparedStatement = connection.prepareStatement(sql)
    preparedStatement.setObject(1, fixedSeriesId, Types.OTHER)
    preparedStatement.executeUpdate()
  }

  def deleteTransferringBody(): Unit = {
    val sql = """DELETE FROM "Body" WHERE "BodyId" = ? """
    val preparedStatement = connection.prepareStatement(sql)
    preparedStatement.setObject(1, fixedBodyId, Types.OTHER)
    preparedStatement.executeUpdate()
  }

  def createFilePropertyValues(propertyName: String, propertyValue: String, default: Boolean, dependencies: Int, secondaryvalue: Int): Unit = {
    val sql = s"""INSERT INTO "FilePropertyValues" ("PropertyName", "PropertyValue", "Default", "Dependencies", "SecondaryValue") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, propertyName)
    ps.setString(2, propertyValue)
    ps.setBoolean(3, default)
    ps.setInt(4, dependencies)
    ps.setInt(5, secondaryvalue)
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

  def createFileProperty(name: String, description: String, fullname: String, propertytype: String,
                         datatype: String, editable: Boolean, mutlivalue: Boolean, propertygroup: String): Unit = {
    val sql =
      s"""INSERT INTO "FileProperty" ("Name", "Description", "CreatedDatetime", "ModifiedDatetime",""" +
        s""" "PropertyType", "Datatype", "Editable", "MutliValue", "PropertyGroup") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)
    ps.setString(2, description)
    ps.setTimestamp(3, Timestamp.from(Instant.now()))
    ps.setTimestamp(4, Timestamp.from(Instant.now()))
    ps.setString(5, propertytype)
    ps.setString(6, datatype)
    ps.setBoolean(7, editable)
    ps.setBoolean(8, mutlivalue)
    ps.setString(9, propertygroup)
    ps.executeUpdate()
  }

  def seedDatabaseWithDefaultEntries(consignmentType: String = "standard"): Unit = {
    val consignmentId = UUID.fromString("eb197bfb-43f7-40ca-9104-8f6cbda88506")
    val seriesId = UUID.fromString("1436ad43-73a2-4489-a774-85fa95daff32")
    createConsignment(consignmentId, userId, seriesId, consignmentType = consignmentType)
    createFile(defaultFileId, consignmentId)

    createClientFileMetadata(defaultFileId)
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


  //scalastyle:off magic.number
  def createConsignment(
                         consignmentId: UUID,
                         userId: UUID,
                         seriesId: UUID = fixedSeriesId,
                         consignmentRef: String = s"TDR-${Instant.now.getNano}-TESTMTB",
                         consignmentType: String = "standard",
                         bodyId: UUID = UUID.fromString("4da472a5-16b3-4521-a630-5917a0722359")): Unit = {
    val sql =
      """INSERT INTO "Consignment" """ +
        """("ConsignmentId", "SeriesId", "UserId", "Datetime", "TransferInitiatedDatetime",
          |"ExportDatetime", "ConsignmentReference", "ConsignmentType", "BodyId", "ConsignmentSequence")""".stripMargin +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val nextValStatement = connection.prepareStatement("select nextval('consignment_sequence_id') as Seq")
    val nextResults: ResultSet = nextValStatement.executeQuery()
    nextResults.next()
    val nextSequence: Int = nextResults.getInt("Seq")

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
  }
  //scalastyle:on magic.number

  def getConsignment(consignmentId: UUID): ResultSet = {
    val sql = s"""SELECT * FROM "Consignment" WHERE "ConsignmentId" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
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
      s"""INSERT INTO "ConsignmentStatus" ("ConsignmentStatusId", "ConsignmentId", "StatusType", "Value", "CreatedDatetime") VALUES (?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, UUID.randomUUID(), Types.OTHER)
    ps.setObject(2, consignmentId, Types.OTHER)
    ps.setString(3, statusType)
    ps.setString(4, statusValue)
    ps.setTimestamp(5, createdDate)
    ps.executeUpdate()
  }

  def getConsignmentStatus(consignmentId: UUID, statusType: String): ResultSet = {
    val sql = """SELECT "Value" FROM "ConsignmentStatus" WHERE "ConsignmentId" = ? AND "StatusType" = ?"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, consignmentId, Types.OTHER)
    ps.setString(2, statusType)
    val result = ps.executeQuery()
    result.next()
    result
  }

  def createFile(fileId: UUID,
                 consignmentId: UUID,
                 fileType: String = NodeType.fileTypeIdentifier,
                 fileName: String = "fileName",
                 parentId: Option[UUID] = None): Unit = {
    val sql = s"""INSERT INTO "File" ("FileId", "ConsignmentId", "UserId", "Datetime", "FileType", "FileName", "ParentId") VALUES (?, ?, ?, ?, ?, ?, ?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setObject(1, fileId, Types.OTHER)
    ps.setObject(2, consignmentId, Types.OTHER)
    ps.setObject(3, userId, Types.OTHER)
    ps.setTimestamp(4, Timestamp.from(FixedTimeSource.now))
    ps.setString(5, fileType)
    ps.setString(6, fileName)
    ps.setObject(7, parentId.map(_.toString).orNull, Types.OTHER)
    ps.executeUpdate()
  }

  //scalastyle:off magic.number
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

  def addFFIDMetadata(fileId: String,
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

  def addFFIDMetadataMatches(ffidMetadataId: String,
                             extension: String = "txt",
                             identificationBasis: String = "TEST DATA identification",
                             puid: String = "TEST DATA puid"
                            ): Unit = {
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
        case ClientSideFileSize => "1"
        case SHA256ClientSideChecksum => "ddb1584d8cb5f07dc6602f58bd5e0184c87e223787af7b619ce04319727b83bf"
        case _ => s"$propertyName Value"
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

  //scalastyle:on magic.number

  def addFileProperty(name: String): Unit = {
    val sql = s"""INSERT INTO "FileProperty" ("Name") VALUES (?)"""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, name)

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

  // scalastyle:off magic.number
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
  // scalastyle:on magic.number

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

  //scalastyle:off magic.number
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
  //scalastyle:on magic.number
}

object DatabaseUtils {
  def apply(connection: Connection): DatabaseUtils = new DatabaseUtils(connection)
}
