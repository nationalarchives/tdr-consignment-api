package uk.gov.nationalarchives.tdr.api.service

import java.time.Instant

/** A clock that can be queried for the time. This is mainly useful so that a fixed time can be injected by tests.
  */
trait TimeSource {
  def now: Instant
}

class CurrentTimeSource extends TimeSource {
  override def now: Instant = Instant.now()
}
