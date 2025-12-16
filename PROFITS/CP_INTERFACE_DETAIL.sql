create table CP_INTERFACE_DETAIL
(
    CP_HASH_CODE  VARCHAR(100) not null,
    INTERNAL_SN   INTEGER      not null,
    TRX_UNIT      INTEGER,
    TRX_DATE      DATE,
    TRX_USR       CHAR(8),
    TRX_USR_SN    INTEGER,
    FIELD_LABEL   VARCHAR(50),
    FIELD_VALUE   VARCHAR(100),
    MANDATORY     CHAR(1),
    TRX_TIMESTAMP TIMESTAMP(6),
    constraint PK_CP_INTDET
        primary key (CP_HASH_CODE, INTERNAL_SN)
);

