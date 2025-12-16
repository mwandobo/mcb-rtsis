create table USRTOT_RECORDING
(
    TMSTAMP             TIMESTAMP(6) not null,
    TRX_UNIT            INTEGER      not null,
    TRX_DATE            DATE         not null,
    TRX_USR             CHAR(8)      not null,
    TRX_USR_SN          INTEGER      not null,
    TUN_INTERNAL_SN     SMALLINT     not null,
    TRX_CODE            INTEGER,
    TRX_JUSTIFIC        INTEGER,
    ID_CURRENCY         INTEGER,
    ID_CURR_TARGET      INTEGER,
    CASH_CR             DECIMAL(15, 2),
    CASH_DB             DECIMAL(15, 2),
    CASH_DIFF           DECIMAL(15, 2),
    JOURNAL_CR          DECIMAL(15, 2),
    JOURNAL_DB          DECIMAL(15, 2),
    JOURNAL_DIFF        DECIMAL(15, 2),
    CHECK_NO            DECIMAL(10),
    ACCOUNT_NUMBER      DECIMAL(11),
    C_DIGIT             SMALLINT,
    NON_FINANCIAL       DECIMAL(15, 2),
    FINANCIAL_IND       INTEGER,
    PRFT_ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_CD          SMALLINT,
    REVERSAL_FLG        CHAR(1),
    PRFT_SYSTEM         SMALLINT,
    COMMENTS            CHAR(100),
    CUST_NAME           CHAR(95),
    VALUE_DATE          DATE,
    CUST_ID             DECIMAL(7),
    constraint IXU_USR_007
        primary key (TMSTAMP, TRX_UNIT, TRX_DATE, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN)
);

create unique index IXN_USR_001
    on USRTOT_RECORDING (TRX_USR);

