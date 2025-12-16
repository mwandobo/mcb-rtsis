create table HISTORY_M_F_DISTRIBUTION
(
    TRX_UNIT         INTEGER  not null,
    TRX_DATE         DATE     not null,
    TRX_USR          CHAR(8)  not null,
    TRX_USR_SN       INTEGER  not null,
    TUN_INTERNAL_SN  SMALLINT not null,
    TRANS_SER_NUM    INTEGER  not null,
    TRX_CODE         INTEGER,
    ACCOUNT_NUMBER   DECIMAL(11),
    MIN_LAST_M_F_PER DECIMAL(8, 4),
    MAX_LAST_DP_PER  DECIMAL(8, 4),
    TMSTAMP          TIMESTAMP(6),
    ENTRY_STATUS     CHAR(1),
    constraint I0000422
        primary key (TRANS_SER_NUM, TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN)
);

