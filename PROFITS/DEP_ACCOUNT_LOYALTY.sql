create table DEP_ACCOUNT_LOYALTY
(
    ACCOUNT_NUMBER DECIMAL(11) not null
        constraint DEP_ACCOUNT_LOYAL_PK
            primary key,
    CUST_ID        INTEGER,
    LAST_TRX_DATE  DATE,
    TMSTAMP        TIMESTAMP(6),
    TRANSACT_COUNT DECIMAL(15, 2)
);

