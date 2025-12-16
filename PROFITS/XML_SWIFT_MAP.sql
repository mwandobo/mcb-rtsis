create table XML_SWIFT_MAP
(
    MT_MSG_TYPE       CHAR(20)  not null,
    MT_SN             SMALLINT  not null,
    XML_NAME_PREFIX   CHAR(10)  not null,
    XML_NAME_URN      CHAR(128) not null,
    XML_NAME_DOC_TYPE CHAR(20)  not null,
    XML_MAP_TYPE      CHAR(1),
    MAP_TYPE          CHAR(10),
    MX_MSG_TYPE       CHAR(20),
    MX_GROUP          CHAR(20),
    AREA              CHAR(40),
    REQUEST_SUB_TYPE  CHAR(40),
    SECTION_GRP       CHAR(2),
    constraint PK_MT_MAP
        primary key (MT_MSG_TYPE, MT_SN, XML_NAME_PREFIX, XML_NAME_URN, XML_NAME_DOC_TYPE)
);

