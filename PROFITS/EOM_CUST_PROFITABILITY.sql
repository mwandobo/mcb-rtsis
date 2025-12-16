create table EOM_CUST_PROFITABILITY
(
    EOM_DATE           TIMESTAMP(6) not null,
    ACCOUNT_NUMERIC    CHAR(40)     not null,
    PROFITS_SUBSYSTEM  DECIMAL(2),
    CUST_ID            DECIMAL(10)  not null,
    PRODUCT_CODE       DECIMAL(5)   not null,
    COMMISSION_NUMERIC DECIMAL(13, 2),
    EXPENSES_NUMERIC   DECIMAL(13, 2),
    PENALTY_NUMERIC    DECIMAL(13, 2),
    DB_INTEREST        DECIMAL(13, 2),
    CR_INTEREST        DECIMAL(13, 2),
    CURRENCY           CHAR(5)      not null,
    constraint EOM_CUST_PROFITABILITY_PK
        primary key (EOM_DATE, ACCOUNT_NUMERIC, CUST_ID, PRODUCT_CODE, CURRENCY)
);

