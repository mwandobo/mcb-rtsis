create table CMS_ATM_RECONC
(
    TERMINAL_ID        CHAR(16)     not null,
    TRX_TIMESTAMP      TIMESTAMP(6) not null,
    AMOUNT_TYPE        CHAR(5),
    TRX_DATE_YYYYMMDD  INTEGER,
    TRAN_TIME_HHMISSTT INTEGER,
    TLF_CRD_PAN        CHAR(16),
    DEBIT_AMNT         DECIMAL(15, 2),
    CREDIT_AMNT        DECIMAL(15, 2),
    LAST_CUTOVER       TIMESTAMP(6),
    TMSTAMP            TIMESTAMP(6),
    ACCOUNTING_DATE    DATE,
    constraint PK_CMS_ATM_RECONC
        primary key (TRX_TIMESTAMP, TERMINAL_ID)
);

