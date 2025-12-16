create table DEL_W_FACT_LOAN_APPLICATION
(
    APPL_ID             CHAR(10)    not null,
    ACCT_KEY            DECIMAL(11) not null,
    APPLICATION_TYPE    CHAR(100),
    APPROVAL_DATE       DATE        not null,
    SALES_PRICE         DECIMAL(15, 2),
    RECEIVE_RENT_AMOUNT DECIMAL(15, 2)
);

create unique index PK_W_FACT_LOAN_APPLICATION
    on DEL_W_FACT_LOAN_APPLICATION (ACCT_KEY, APPL_ID, APPLICATION_TYPE);

