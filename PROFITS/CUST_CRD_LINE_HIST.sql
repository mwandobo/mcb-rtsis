create table CUST_CRD_LINE_HIST
(
    CUST_ID           INTEGER     not null,
    ID_CURRENCY       INTEGER     not null,
    FK_GENERIC_DETAIL INTEGER     not null,
    HISTORY_SN        DECIMAL(10) not null,
    PRFT_SYSTEM       SMALLINT,
    TRX_INTERNAL_SN   SMALLINT,
    TRX_CODE          INTEGER,
    TRX_UNIT          INTEGER,
    TRX_SN            INTEGER,
    UTILISED_AMOUNT   DECIMAL(15, 2),
    CRLINE_AMOUNT     DECIMAL(15, 2),
    REEVALUATION_DT   DATE,
    EVALUATION_DT     DATE,
    TRX_DATE          DATE,
    TMSTAMP           TIMESTAMP(6),
    TRX_USER          CHAR(8),
    ACCOUNT_NUMBER    CHAR(40)
);

create unique index IXU_CUS_039
    on CUST_CRD_LINE_HIST (CUST_ID, ID_CURRENCY, FK_GENERIC_DETAIL, HISTORY_SN);

