create table MESSAGE_FORECAST_ERROR
(
    SUBTAG         SMALLINT     not null,
    ERROR_SN       INTEGER      not null,
    MESSAGE_SN     INTEGER      not null,
    TAG            CHAR(10)     not null,
    MESSAGE_TYPE   CHAR(20)     not null,
    TMSTAMP        TIMESTAMP(6) not null,
    EXCEPTION_CODE CHAR(10),
    ACTION_BLOCK   CHAR(40),
    ERROR_COMMENT  CHAR(80),
    FILE_LINE      INTEGER,
    constraint IXU_CP_117
        primary key (SUBTAG, ERROR_SN, MESSAGE_SN, TAG, MESSAGE_TYPE, TMSTAMP)
);

