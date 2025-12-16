create table COS_MEMBER_ERROR
(
    MEMBER_ID       DECIMAL(10) not null,
    ERROR_ID        DECIMAL(12) not null
        constraint PK_MEMB_ERR
            primary key,
    ID_PRODUCT      INTEGER     not null,
    ID_TRANSACT     INTEGER     not null,
    ID_JUSTIFIC     INTEGER,
    TRX_DATE        DATE,
    TRX_USR         CHAR(8),
    TRX_UNIT        INTEGER,
    MONITORING_UNIT INTEGER,
    ERROR_MSG       VARCHAR(100),
    TMSTAMP         TIMESTAMP(6),
    SUBFLAG_FROM    SMALLINT,
    SUBFLAG_TO      SMALLINT
);

