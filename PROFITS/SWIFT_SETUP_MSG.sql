create table SWIFT_SETUP_MSG
(
    MESSAGE_TYPE        CHAR(20) not null
        constraint IXU_SWI_000
            primary key,
    DEFAULT_TAG_SETUP   CHAR(1),
    MSG_TYPE            CHAR(2),
    DESCRIPTION         CHAR(100),
    MESSAGE_DESCRIPTION VARCHAR(4000),
    BASE_MESSAGE_TYPE   VARCHAR(20),
    INSTRUM_CODE        CHAR(10),
    XML_MESSAGE_TYPE    VARCHAR(20),
    IO_DIRECTION        CHAR(1)
);

