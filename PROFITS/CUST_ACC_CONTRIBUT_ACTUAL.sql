create table CUST_ACC_CONTRIBUT_ACTUAL
(
    CUST_ID          INTEGER  not null,
    ACCOUNT_NUMBER   CHAR(40) not null,
    PRFT_SYSTEM      SMALLINT not null,
    MONTH            SMALLINT not null,
    YEAR             SMALLINT not null,
    TRX_DATE         DATE,
    CONTRIBUTION_AMN DECIMAL(18, 2),
    UTILIZED_AMOUNT  DECIMAL(18, 2)
);

create unique index IXP_CUST_ACC_CONTR_ACC
    on CUST_ACC_CONTRIBUT_ACTUAL (CUST_ID, ACCOUNT_NUMBER, PRFT_SYSTEM, MONTH, YEAR);

