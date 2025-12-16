create table CLC_PAY_PROMISES_TUN
(
    CASE_ID        CHAR(40)          not null,
    CUST_ID        INTEGER           not null,
    PAY_NUMBER     INTEGER           not null,
    PAID_AMN       DECIMAL(18, 2),
    TRX_DATE       DATE,
    TRX_UNIT       INTEGER,
    TRX_USR        CHAR(8),
    TRX_SN         INTEGER,
    TMSTAMP        TIMESTAMP(6)      not null,
    ACCOUNT_NUMBER CHAR(40),
    PRFT_SYSTEM    INTEGER,
    RF_CODE        CHAR(15),
    REVERSAL_FLG   CHAR(1),
    ENTRY_COMMENTS CHAR(40),
    ENTRY_STATUS   CHAR(1),
    PAY_PROMISE_ID INTEGER default 0 not null,
    constraint PK_CLC_PROM_TUN
        primary key (CASE_ID, CUST_ID, PAY_NUMBER, PAY_PROMISE_ID, TMSTAMP)
);

