create table XML_I_SWT_MAP_TAG
(
    MAP_TYPE         CHAR(10)    not null,
    MT_MSG_TYPE      CHAR(20)    not null,
    MT_TAG           VARCHAR(50) not null,
    MT_SN            SMALLINT    not null,
    MT_TAG_SAME_LINE CHAR(1),
    MX_MSG_TYPE      CHAR(20)    not null,
    MX_TAG           VARCHAR(50) not null,
    HASHED_XPATH     VARCHAR(500),
    MT_TYPE          VARCHAR(40),
    MT_ATTRIBUTE     VARCHAR(20),
    MT_VALUE         VARCHAR(40),
    MT_VALUE_TYPE    CHAR(20),
    MT_REUSE_VALUE   CHAR(20),
    constraint PK1_MT_TO_MX
        primary key (MAP_TYPE, MT_MSG_TYPE, MT_TAG, MT_SN, MX_TAG, MX_MSG_TYPE)
);

