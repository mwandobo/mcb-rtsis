create table TEMP_REP_73879
(
    ACCOUNT_NUMBER CHAR(40) not null,
    TRX_DATE       DATE     not null,
    PRODUCT        INTEGER,
    UNIT           INTEGER,
    DP_BALANCE_FC  DECIMAL(15, 2),
    DP_BALANCE_DC  DECIMAL(15, 2),
    DP_RATE        DECIMAL(8, 4),
    LNS_BAL_FC     DECIMAL(15, 2),
    LNS_BAL_DC     DECIMAL(15, 2),
    LNS_RATE       DECIMAL(8, 4),
    ACC_CURR       INTEGER,
    constraint PK_TEMP_REP_73789
        primary key (TRX_DATE, ACCOUNT_NUMBER)
);

