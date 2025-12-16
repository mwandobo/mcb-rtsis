create table LG_HISTORY
(
    TMSTAMP             TIMESTAMP(6) not null,
    TRX_INTERNAL_SN     SMALLINT,
    TRX_CODE            INTEGER,
    TRX_UNIT            INTEGER,
    ID_JUSTIFIC         INTEGER,
    LG_AMN_TOLERANCE    DECIMAL(8, 4),
    TRX_SN              INTEGER,
    FK_LG_ACCOUNTACC_SN DECIMAL(13),
    TRX_AMN             DECIMAL(15, 2),
    ACCOUNT_LIMIT_AMN   DECIMAL(15, 2),
    TRX_EXPENSES        DECIMAL(15, 2),
    TRX_COMMISSION      DECIMAL(15, 2),
    TRX_DATE            DATE,
    DATE_TO             DATE,
    DATE_FROM           DATE,
    ACCOUNT_STATUS      CHAR(1),
    OBLIGATION_STATUS   CHAR(1),
    TRX_USR             CHAR(8)
);

create unique index IXP_LG__004
    on LG_HISTORY (FK_LG_ACCOUNTACC_SN, TMSTAMP, TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TRX_INTERNAL_SN);

