create table LNS_EXT_SUBSIDY_REC
(
    TRX_DATE         DATE,
    ACCOUNT_NUMBER   CHAR(40)     not null,
    APPLICATION_NO   CHAR(40)     not null,
    DEBT_NUMBER      CHAR(40)     not null,
    PAYMENT_NO       CHAR(40)     not null,
    PAYMENT_AMOUNT   DECIMAL(15, 2),
    TMSTAMP          TIMESTAMP(6) not null,
    REQUEST_SN       INTEGER,
    REQUEST_TYPE     CHAR(1),
    REQUEST_LOAN_STS CHAR(1),
    constraint PK_SUB_REC
        primary key (ACCOUNT_NUMBER, APPLICATION_NO, DEBT_NUMBER, PAYMENT_NO, TMSTAMP)
);

