create table XML_INPUT_FILE
(
    ID                       DECIMAL(15)  not null
        constraint XMLIFIPK
            primary key,
    TRX_DATE                 DATE,
    FILE_NAME                CHAR(250)    not null,
    PROFITS_FILE_TABLE_REF   DECIMAL(15),
    TIMESTAMP_CREATED        TIMESTAMP(6) not null,
    TIMESTAMP_PROCESSED      TIMESTAMP(6),
    IO_DIRECTION             CHAR(1)      not null,
    COMPLETE_STAGE           CHAR(2)      not null,
    FILE_CONTENT             BLOB(524288000),
    FK_XMLNAMESPACE_DOC_TYPE CHAR(20),
    FK_XMLNAMESPACE_URN      CHAR(128),
    FK_XMLNAMESPACE_PREFIX   CHAR(10),
    DELETED                  SMALLINT     not null,
    INDEX1                   INTEGER,
    INDEX2                   INTEGER,
    INDEX3                   INTEGER,
    INDEX4                   INTEGER,
    INDEX5                   INTEGER,
    INDEX6                   INTEGER,
    FK_ORDER_CODE            VARCHAR(20),
    FK_XML_ENTITY_ID         DECIMAL(10),
    ID_JUSTIFIC              INTEGER,
    SETUP_ID                 CHAR(20),
    SETUP_JUSTIFIC           INTEGER,
    DATA_ROW_SN              DECIMAL(10)
);

comment on column XML_INPUT_FILE.ID is 'Uninue ID, a number from an ascending sequence of integers';

comment on column XML_INPUT_FILE.PROFITS_FILE_TABLE_REF is 'Foreign Key to PROFITS Tradiotional file, which may contaim a lot of particular business data';

comment on column XML_INPUT_FILE.FK_ORDER_CODE is 'Relates to IPS_MESSAGE_HEADER.ORDER_CODE.It is used to retrieve normal or orinal order (e.g. Pacs008, Pacs004 etc).';

comment on column XML_INPUT_FILE.ID_JUSTIFIC is 'It is a unique number that identifies a specificjustification.*****************Comments****************** JU Code';

comment on column XML_INPUT_FILE.SETUP_JUSTIFIC is '     PIG     ( detail records)        .';

comment on column XML_INPUT_FILE.DATA_ROW_SN is 'The serial Number of the loaded line, only for the details part of the file.';

create unique index XMLIFII1
    on XML_INPUT_FILE (IO_DIRECTION, TIMESTAMP_CREATED, FILE_NAME);

create unique index XMLIFII2
    on XML_INPUT_FILE (FK_XML_ENTITY_ID);

create unique index XMLIFII3
    on XML_INPUT_FILE (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX);

