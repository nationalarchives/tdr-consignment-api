package uk.gov.nationalarchives.tdr.api.utils

import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * This trait should be mixed into specs which access the test database.
 *
 * It provides a test database connection and cleans up all of the test data after every test.
 */
trait TestDatabase extends BeforeAndAfterEach {
  this: Suite =>

}
