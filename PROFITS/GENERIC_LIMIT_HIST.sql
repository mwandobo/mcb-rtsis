create table GENERIC_LIMIT_HIST
(
    FK_GENERIC_LIM_ID DECIMAL(10),
    LIMIT_HIST_SN     DECIMAL(10),
    PRFT_SYSTEM       SMALLINT,
    ID_CURRENCY       INTEGER,
    TRX_UNIT          INTEGER,
    TRX_CODE          INTEGER,
    TRX_SN            INTEGER,
    CUST_ID           INTEGER,
    AVAIL_LIMIT_AMN   DECIMAL(15, 2),
    TRX_AMN_LC        DECIMAL(15, 2),
    TRX_AMN           DECIMAL(15, 2),
    TRX_DATE          DATE,
    TMSTAMP           TIMESTAMP(6),
    REVERSED_FLG      CHAR(1),
    TRX_USER          CHAR(8),
    ACCOUNT_NUMBER    CHAR(40),
    TRX_COMMENTS      CHAR(40)
);

create unique index IXU_GEN_005
    on GENERIC_LIMIT_HIST (FK_GENERIC_LIM_ID, LIMIT_HIST_SN);

