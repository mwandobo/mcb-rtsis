create table TRS_LIMITS_EXCEED
(
    TRX_DATE          DATE not null,
    SEQ_SN            DECIMAL(10) generated always as identity,
    TRX_AMOUNT_LC     DECIMAL(15, 1),
    DEALER_CODE       CHAR(8),
    USR               CHAR(8),
    BANK_ID           INTEGER,
    CNTRY_PARAM_TYPE  CHAR(5),
    CNTRY_SERIAL_NUM  INTEGER,
    CNTRY_SHORT_DESCR CHAR(10),
    LIMIT_PARAM_TYPE  CHAR(5),
    LIMIT_SERIAL_NUM  INTEGER,
    LIMIT_SHORT_DESCR CHAR(10),
    ONE_TRX_LIMIT     DECIMAL(15, 2),
    AGREEMENT_LIMIT   DECIMAL(15, 2),
    UTILISED_LIMIT    DECIMAL(15, 2),
    TOTAL_LIMIT       DECIMAL(15, 2),
    AVAILABLE_LIMIT   DECIMAL(15, 2),
    MATURITY_DATE     DATE,
    TMSTAMP           TIMESTAMP(6),
    constraint PK_LIMITS_EXCEDED
        primary key (SEQ_SN, TRX_DATE)
);

