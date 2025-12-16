create table IPS_MESSAGE_ERRORS
(
    TMSTAMP         TIMESTAMP(6) not null,
    ORDER_CODE      VARCHAR(20)  not null,
    ERROR_SN        INTEGER      not null,
    MESSAGE_TYPE    CHAR(20)     not null,
    TAG             CHAR(10)     not null,
    SUBTAG          SMALLINT     not null,
    MESSAGE_SN      INTEGER      not null,
    EXCEPTION_CODE  CHAR(10),
    ACTION_BLOCK    CHAR(40),
    ERROR_COMMENT   CHAR(80),
    ORDER_FILENAME  VARCHAR(50),
    ORDER_FILE_LINE INTEGER,
    SEVERITY_LEVEL  SMALLINT,
    constraint IXU_CP__59
        primary key (MESSAGE_SN, SUBTAG, TAG, MESSAGE_TYPE, ERROR_SN, ORDER_CODE, TMSTAMP)
);

create unique index IXN_IPS_MSG_ERR_01
    on IPS_MESSAGE_ERRORS (ORDER_FILENAME);

