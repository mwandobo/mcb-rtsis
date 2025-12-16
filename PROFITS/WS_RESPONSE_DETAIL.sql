create table WS_RESPONSE_DETAIL
(
    KEY_REF_NUMBER CHAR(100)   not null,
    KEY_WS_CODE    VARCHAR(20) not null,
    INTERNAL_SN    INTEGER     not null,
    TRX_TMSTAMP    TIMESTAMP(6),
    FIELD_LABEL    VARCHAR(50),
    FIELD_VALUE    VARCHAR(100),
    constraint PK_WS_RESPONSE_DETAIL
        primary key (KEY_REF_NUMBER, KEY_WS_CODE, INTERNAL_SN)
);

