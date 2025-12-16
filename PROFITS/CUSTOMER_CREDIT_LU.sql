create table CUSTOMER_CREDIT_LU
(
    FK_GENERIC_DETAFK  CHAR(5)        not null,
    FK_GENERIC_DETASER INTEGER        not null,
    FK_CURRENCYID_CURR INTEGER        not null,
    FK_CUSTOMERCUST_ID INTEGER        not null,
    FK_UNITCODE        INTEGER,
    FK_USRCODE         CHAR(8),
    EVALUATION_DT      DATE,
    REEVALUATION_DT    DATE,
    CRLINE_AMOUNT      DECIMAL(15, 2) not null,
    EXPIRY_DATE        DATE           not null,
    UTILISED_AMOUNT    DECIMAL(15, 2) not null,
    ENTRY_STATUS       CHAR(1)        not null,
    HISTORY_CNT        DECIMAL(10),
    TMSTAMP            TIMESTAMP(6)   not null,
    constraint IXU_CIU_015
        primary key (TMSTAMP, FK_CUSTOMERCUST_ID, FK_CURRENCYID_CURR, FK_GENERIC_DETAFK, FK_GENERIC_DETASER)
);

