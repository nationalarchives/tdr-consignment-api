package uk.gov.nationalarchives.tdr.api.utils

import java.util.UUID

import uk.gov.nationalarchives.tdr.api.service.UUIDSource

class FixedUUIDSource extends UUIDSource {
  var idx = -1
  override def uuid: UUID = {
    idx += 1
    List(
      UUID.fromString("6e3b76c4-1745-4467-8ac5-b4dd736e1b3e"),
      UUID.fromString("7e3b76c4-1745-4467-8ac5-b4dd736e1b3e"),
      UUID.fromString("8e3b76c4-1745-4467-8ac5-b4dd736e1b3e"),
      UUID.fromString("9e3b76c4-1745-4467-8ac5-b4dd736e1b3e"),
      UUID.fromString("47e365a4-fc1e-4375-b2f6-dccb6d361f5f"),
      UUID.fromString("0824d319-9c58-4b15-81eb-b71fb460fa36"),
      UUID.fromString("15eddae1-f319-4da1-b2da-cda3bd8f840a"),
      UUID.fromString("854b1c99-5998-4126-a209-6f1bcc38bf74"),
      UUID.fromString("8b5baaa6-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bae20-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb050-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb226-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb406-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb546-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb708-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bb898-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bba6e-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbc44-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbd98-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbf64-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbf64-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbf64-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbf64-5f12-11eb-ae93-0242ac130002"),
      UUID.fromString("8b5bbf64-5f12-11eb-ae93-0242ac130002")
    )(idx)
  }

  def reset: Unit = idx = -1

}
