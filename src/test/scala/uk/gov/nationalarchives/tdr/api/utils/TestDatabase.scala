package uk.gov.nationalarchives.tdr.api.utils

import java.sql.Connection

import org.scalatest.{BeforeAndAfterEach, Suite}
import uk.gov.nationalarchives.tdr.api.db.DbConnection
import uk.gov.nationalarchives.tdr.api.service.FileMetadataService.{clientSideProperties, staticMetadataProperties}
import uk.gov.nationalarchives.tdr.api.service.TransferAgreementService.transferAgreementProperties
import uk.gov.nationalarchives.tdr.api.service.FinalTransferConfirmationService.finalTransferConfirmationProperties
import uk.gov.nationalarchives.tdr.api.utils.TestUtils.{addConsignmentProperty, addFileProperty}

/**
 * This trait should be mixed into specs which access the test database.
 *
 * It provides a test database connection and cleans up all of the test data after every test.
 */
trait TestDatabase extends BeforeAndAfterEach {
  this: Suite =>

  val databaseConnection: Connection = DbConnection.db.source.createConnection()

  override def beforeEach(): Unit = {
    databaseConnection.prepareStatement("DELETE FROM FileStatus").execute()
    databaseConnection.prepareStatement("DELETE FROM FileMetadata").execute()
    databaseConnection.prepareStatement("DELETE FROM FileProperty").execute()
    databaseConnection.prepareStatement("DELETE FROM FFIDMetadataMatches").execute()
    databaseConnection.prepareStatement("DELETE FROM FFIDMetadata").execute()
    databaseConnection.prepareStatement("DELETE FROM AVMetadata").execute()
    databaseConnection.prepareStatement("DELETE FROM File").execute()
    databaseConnection.prepareStatement("DELETE FROM ConsignmentMetadata").execute()
    databaseConnection.prepareStatement("DELETE FROM ConsignmentProperty").execute()
    databaseConnection.prepareStatement("DELETE FROM ConsignmentStatus").execute()
    databaseConnection.prepareStatement("DELETE FROM Consignment").execute()
    databaseConnection.prepareStatement("DELETE FROM Series").execute()
    databaseConnection.prepareStatement("DELETE FROM Body").execute()
    databaseConnection.prepareStatement("ALTER SEQUENCE consignment_sequence_id RESTART WITH 1").execute()

    databaseConnection.prepareStatement("INSERT INTO FileProperty (Name, Description, Shortname) " +
      "VALUES ('SHA256ServerSideChecksum', 'The checksum calculated after upload', 'Checksum')")
      .execute()

    addTransferAgreementConsignmentProperties()
    addTransferAgreementFileProperties()
    addFinalTransferConfirmationProperties()
    addClientSideProperties()
  }

  private def addTransferAgreementConsignmentProperties(): Unit = {
    transferAgreementProperties.foreach(propertyName => {
      addConsignmentProperty(propertyName)
    })
  }

  private def addTransferAgreementFileProperties(): Unit = {
    staticMetadataProperties.foreach(propertyName => {
      addFileProperty(propertyName.name)
    })
  }

  private def addFinalTransferConfirmationProperties(): Unit = {
    finalTransferConfirmationProperties.foreach(propertyName => {
      addConsignmentProperty(propertyName)
    })
  }

  private def addClientSideProperties(): Unit = {
    clientSideProperties.foreach(propertyName => {
      addFileProperty(propertyName)
    })
  }
}
