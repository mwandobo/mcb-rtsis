create table WS_RECORDING_DETAIL
(
    TRX_UNIT      INTEGER not null,
    TRX_DATE      DATE    not null,
    TRX_USR       CHAR(8) not null,
    TRX_USR_SN    INTEGER not null,
    INTERNAL_SN   INTEGER not null,
    FIELD_LABEL   VARCHAR(50),
    FIELD_VALUE   VARCHAR(100),
    TRX_TIMESTAMP TIMESTAMP(6),
    DETAIL_SN     SMALLINT,
    FIELD_SN      SMALLINT,
    MANDATORY     CHAR(1),
    constraint PK_WS_RECORDING_DETAIL
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN, INTERNAL_SN)
);

