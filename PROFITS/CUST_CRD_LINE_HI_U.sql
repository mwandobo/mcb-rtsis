create table CUST_CRD_LINE_HI_U
(
    HISTORY_SN        DECIMAL(10) not null,
    CUST_ID           INTEGER     not null,
    ID_CURRENCY       INTEGER     not null,
    FK_GENERIC_DETAIL INTEGER     not null,
    TMSTAMP           TIMESTAMP(6),
    ACCOUNT_NUMBER    CHAR(40),
    PRFT_SYSTEM       SMALLINT,
    CRLINE_AMOUNT     DECIMAL(15, 2),
    UTILISED_AMOUNT   DECIMAL(15, 2),
    TRX_DATE          DATE,
    TRX_UNIT          INTEGER,
    TRX_USER          CHAR(8),
    TRX_CODE          INTEGER,
    TRX_SN            INTEGER,
    TRX_INTERNAL_SN   SMALLINT,
    EVALUATION_DT     DATE,
    REEVALUATION_DT   DATE,
    constraint IXU_CIU_027
        primary key (FK_GENERIC_DETAIL, ID_CURRENCY, CUST_ID, HISTORY_SN)
);

