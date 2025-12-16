create table WS_RESPONSE
(
    KEY_REF_NUMBER   CHAR(100)   not null,
    KEY_WS_CODE      VARCHAR(20) not null,
    TRX_UNIT         INTEGER,
    TRX_DATE         DATE,
    TRX_USR          CHAR(8),
    TRX_USR_SN       INTEGER,
    INTERNAL_SN      INTEGER,
    TRX_TMSTAMP      TIMESTAMP(6),
    RESPONSE_MESSAGE BLOB(1073741824),
    INPUT_PARAMETERS VARCHAR(4000),
    constraint PK_WS_RESPONSE
        primary key (KEY_REF_NUMBER, KEY_WS_CODE)
);

