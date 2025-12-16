create table COLL_TRX_RECORDING
(
    TRX_UNIT            INTEGER  not null,
    TRX_DATE            DATE     not null,
    TRX_USR             CHAR(8)  not null,
    TRX_USR_SN          INTEGER  not null,
    TUN_INTERNAL_SN     SMALLINT not null,
    TRX_CODE            INTEGER,
    CHANNEL_ID          INTEGER,
    TERMINAL_NUMBER     CHAR(99),
    PRODUCTION_DATE     DATE,
    TRANSACTION_STATUS  CHAR(1),
    TRN_TYPE            CHAR(1),
    AUTHORIZER1         CHAR(8),
    AUTHORIZER2         CHAR(8),
    ID_PRODUCT          INTEGER,
    ID_JUSTIFIC         INTEGER,
    ACC_AMOUNT_1        DECIMAL(15, 2),
    ID_CURRENCY_1       INTEGER,
    ACC_AMOUNT_2        DECIMAL(15, 2),
    ID_CURRENCY_2       INTEGER,
    ACC_AMOUNT_3        DECIMAL(15, 2),
    ID_CURRENCY_3       INTEGER,
    ACC_AMOUNT_4        DECIMAL(15, 2),
    ID_CURRENCY_4       INTEGER,
    TOTALS_CURRENCY     INTEGER,
    JOURNAL_DB_TOT_AMNT DECIMAL(15, 2),
    JOURNAL_CR_TOT_AMNT DECIMAL(15, 2),
    PRODUCTION_TIME     TIME,
    constraint PK_COL_RECORDING
        primary key (TUN_INTERNAL_SN, TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

