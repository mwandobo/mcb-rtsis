create table HIST_XML_INPUT_FILE
(
    ID                       DECIMAL(15)  not null
        constraint HISTXMLIFIPK
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

create unique index HISTXMLIFII1
    on HIST_XML_INPUT_FILE (IO_DIRECTION, TIMESTAMP_CREATED, FILE_NAME);

create unique index HISTXMLIFII2
    on HIST_XML_INPUT_FILE (FK_XML_ENTITY_ID);

create unique index HISTXMLIFII3
    on HIST_XML_INPUT_FILE (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX);

