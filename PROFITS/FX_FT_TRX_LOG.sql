create table FX_FT_TRX_LOG
(
    TRX_USR           CHAR(8)      not null,
    TRX_SN            INTEGER      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    TRX_UNIT          INTEGER,
    CHANNEL_ID        INTEGER,
    CHANNEL_USER      VARCHAR(40),
    PROFITS_UNIQUE_ID VARCHAR(40),
    REFERENCE_NUMBER  CHAR(40),
    S_ACC_TYPE        SMALLINT,
    S_ACC_SN          DECIMAL(11),
    S_ACC_UNIT        INTEGER,
    T_ACC_TYPE        SMALLINT,
    T_ACC_SN          DECIMAL(11),
    T_ACC_UNIT        INTEGER,
    START_END         CHAR(1),
    EXIT_MESSAGE      VARCHAR(80),
    constraint PK_FXFT_LOG
        primary key (TRX_USR, TRX_SN)
);

