CREATE SCHEMA IF NOT EXISTS consignmentapi;

CREATE TABLE IF NOT EXISTS consignmentapi.Series (
  BodyId UUID DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  Name varchar(255) DEFAULT NULL,
  Code varchar(255) DEFAULT NULL,
  Description varchar(255) DEFAULT NULL,
  SeriesId UUID NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  PRIMARY KEY (SeriesId)
);

CREATE TABLE IF NOT EXISTS consignmentapi.Body (
   BodyId UUID not null DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   Name varchar(255) default null,
   Code varchar(255) default null,
   Description varchar(255) default null,
   PRIMARY KEY (BodyId)
);

CREATE TABLE IF NOT EXISTS consignmentapi.Consignment (
  ConsignmentId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  SeriesId uuid DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  UserId uuid DEFAULT NULL,
  Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (ConsignmentId)
);

CREATE TABLE IF NOT EXISTS consignmentapi.TransferAgreement (
  ConsignmentId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  AllPublicRecords BOOLEAN DEFAULT NULL,
  AllCrownCopyright BOOLEAN DEFAULT NULL,
  AllEnglish BOOLEAN DEFAULT NULL,
  AllDigital BOOLEAN DEFAULT NULL,
  AppraisalSelectionSignedOff BOOLEAN DEFAULT NULL,
  SensitivityReviewSignedOff BOOLEAN DEFAULT NULL,
  TransferAgreementId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
PRIMARY KEY (TransferAgreementId)
);

CREATE TABLE IF NOT EXISTS consignmentapi.ClientFileMetadata (
   FileId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   OriginalPath varchar(255) DEFAULT NULL,
   Checksum varchar(255) DEFAULT NULL,
   ChecksumType varchar(255) DEFAULT NULL,
   LastModified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   CreatedDate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   Filesize bigint DEFAULT NULL,
   Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   ClientFileMetadataId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   PRIMARY KEY (ClientFileMetadataId)
);

CREATE TABLE IF NOT EXISTS consignmentapi.File (
   FileId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   ConsignmentId uuid NOT NULL,
   UserId uuid DEFAULT NULL,
   Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (FileId)
);

DELETE from consignmentapi.Body;
INSERT INTO consignmentapi.Body (BodyId, Name, Code, Description) VALUES ('6e3b76c4-1745-4467-8ac5-b4dd736e1b3e', 'Body', 'Code', 'Description'), ('645bee46-d738-439b-8007-2083bc983154', 'Body2', 'Code', 'Description');
