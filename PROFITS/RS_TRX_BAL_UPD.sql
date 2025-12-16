create table RS_TRX_BAL_UPD
(
    TMSTAMP             TIMESTAMP(6)   not null,
    TRX_UNIT            INTEGER        not null,
    TRX_DATE            DATE           not null,
    TRX_USR             CHAR(8)        not null,
    TRX_USR_SN          INTEGER        not null,
    ACCOUNT_NUMBER      CHAR(40)       not null,
    ACCOUNT_CD          SMALLINT       not null,
    ACCOUNT_PRFT_SYSTEM SMALLINT       not null,
    TUN                 CHAR(110)      not null,
    P_CHANNEL           CHAR(20)       not null,
    P_MOD_ID            SMALLINT       not null,
    ACC_MONITOR_UNIT    INTEGER        not null,
    TRX_AMOUNT          DECIMAL(15, 2) not null,
    TRX_CODE            INTEGER        not null,
    P_ACCOUNT           CHAR(40)       not null,
    P_CURRENCY          CHAR(3)        not null,
    P_HOLDID            INTEGER,
    P_ERRCODE           SMALLINT       not null,
    constraint PK_RS_TRX_BAL
        primary key (ACCOUNT_PRFT_SYSTEM, ACCOUNT_NUMBER, TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT, TMSTAMP)
);

