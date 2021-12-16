CREATE SEQUENCE IF NOT EXISTS consignment_sequence_id
    START WITH 1
    NO CYCLE
    INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS Series (
  BodyId UUID DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  Name varchar(255) DEFAULT NULL,
  Code varchar(255) DEFAULT NULL,
  Description varchar(255) DEFAULT NULL,
  SeriesId UUID NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  PRIMARY KEY (SeriesId)
);

CREATE TABLE IF NOT EXISTS Body (
   BodyId UUID not null DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   Name varchar(255) default null,
   Code varchar(255) default null,
   TdrCode varchar(255) default null,
   Description varchar(255) default null,
   PRIMARY KEY (BodyId)
);

CREATE TABLE IF NOT EXISTS Consignment (
  ConsignmentId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  SeriesId uuid DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  UserId uuid DEFAULT NULL,
  Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ParentFolder varchar(255),
  TransferInitiatedDatetime timestamp with time zone,
  TransferInitiatedBy uuid,
  ExportDatetime timestamp with time zone,
  ExportLocation text,
  ConsignmentSequence bigint DEFAULT NEXT VALUE FOR consignment_sequence_id,
  ConsignmentReference varchar(255) NOT NULL,
  ConsignmentType text NOT NULL,
  BodyId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
  PRIMARY KEY (ConsignmentId)
);

CREATE TABLE IF NOT EXISTS ConsignmentProperty (
  Name varchar(255),
  Description varchar(255),
  Shortname varchar(255),
  PRIMARY KEY (Name)
);

CREATE TABLE IF NOT EXISTS ConsignmentMetadata (
    MetadataId uuid NOT NULL,
    ConsignmentId uuid NOT NULL,
    PropertyName text NOT NULL,
    Value varchar(255),
    Datetime timestamp not null DEFAULT CURRENT_TIMESTAMP,
    UserId uuid NOT NULL,
    PRIMARY KEY (MetadataId)
);

CREATE TABLE IF NOT EXISTS File (
   FileId uuid NOT NULL DEFAULT '6e3b76c4-1745-4467-8ac5-b4dd736e1b3e',
   ConsignmentId uuid NOT NULL,
   UserId uuid DEFAULT NULL,
   ChecksumMatches boolean,
   Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   "FileType" text,
   "FileName" text,
   "ParentId" uuid,
   PRIMARY KEY (FileId)
);

CREATE TABLE IF NOT EXISTS AVMetadata (
    FileId uuid DEFAULT NULL,
    Software varchar(255) NOT NULL,
    SoftwareVersion varchar(255) NOT NULL,
    DatabaseVersion varchar(255) NOT NULL,
    Result varchar(255) NOT NULL,
    Datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS FileProperty (
    Name varchar(255),
    Description varchar(255),
    Shortname varchar(255),
    PRIMARY KEY (Name)
);

CREATE TABLE IF NOT EXISTS FileMetadata (
    MetadataId uuid,
    FileId uuid not null,
    PropertyId uuid,
    PropertyName varchar(255),
    Value varchar(255) not null,
    Datetime timestamp not null DEFAULT CURRENT_TIMESTAMP,
    UserId uuid NOT NULL,
    PRIMARY KEY (MetadataId),
    FOREIGN KEY (FileId) REFERENCES File(FileId),
    FOREIGN KEY (PropertyName) REFERENCES FileProperty(Name)
);

CREATE TABLE IF NOT EXISTS FFIDMetadata (
    FFIDMetadataId uuid not null,
    FileId uuid NOT NULL,
    Software varchar(255) not null,
    SoftwareVersion varchar(255) not null,
    BinarySignatureFileVersion varchar(255) not null,
    ContainerSignatureFileVersion varchar(255) not null,
    Method varchar(255) not null,
    Datetime timestamp not null,
    PRIMARY KEY(FFIDMetadataId),
    FOREIGN KEY (FileId) REFERENCES File(FileId)
);

CREATE TABLE IF NOT EXISTS FFIDMetadataMatches (
    FFIDMetadataId uuid not null,
    Extension varchar(255),
    IdentificationBasis varchar(255) not null,
    PUID varchar(255) not null,
    FOREIGN KEY (FFIDMetadataId) REFERENCES FFIDMetadata(FFIDMetadataId)
);

CREATE TABLE IF NOT EXISTS ConsignmentStatus (
    ConsignmentStatusId uuid not null,
    ConsignmentId uuid not null,
    StatusType varchar(255) not null,
    Value varchar(255) not null,
    CreatedDatetime timestamp not null,
    ModifiedDatetime timestamp,
    PRIMARY KEY (ConsignmentStatusId),
    FOREIGN KEY (ConsignmentId) REFERENCES Consignment(ConsignmentId)
);

CREATE TABLE IF NOT EXISTS "FileStatus"
(
    FileStatusId uuid NOT NULL,
    FileId uuid NOT NULL,
    StatusType text NOT NULL,
    Value text NOT NULL,
    CreatedDatetime  timestamp with time zone not null,
    PRIMARY KEY (FileStatusId),
    FOREIGN KEY (FileId) REFERENCES File(FileId)

);
