create table XML_O_SWT_MAP_TAG
(
    MAP_TYPE          CHAR(10) not null,
    MT_MSG_TYPE       CHAR(20) not null,
    MT_TAG            CHAR(10) not null,
    MT_SN             SMALLINT not null,
    MT_TYPE           VARCHAR(40),
    MT_MULTI_LINE     CHAR(1),
    MT_CODE_FIND      CHAR(20),
    MT_CODE_OCCURS    SMALLINT,
    MT_RESTART_OCCURS CHAR(1),
    MT_VALUE_TYPE     CHAR(7),
    MT_START_POS      SMALLINT,
    MT_LENGTH         SMALLINT,
    MX_MSG_TYPE       CHAR(20) not null,
    MX_TAG            VARCHAR(50),
    HASHED_XPATH      VARCHAR(500),
    ORDER0            DECIMAL(15),
    HASH_KEY          DECIMAL(15),
    HASH_SEQ_ID       INTEGER,
    MX_GROUP          CHAR(20),
    constraint PK_XML_O_MAP_TAG
        primary key (MT_MSG_TYPE, MAP_TYPE, MT_SN, MT_TAG)
);

