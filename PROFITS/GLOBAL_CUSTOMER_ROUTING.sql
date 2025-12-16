create table GLOBAL_CUSTOMER_ROUTING
(
    SN             DECIMAL(8) not null
        constraint PKX_GLOBAL_CROUT
            primary key,
    CUSTOMER_ID    DECIMAL(7),
    CUSTOMER_CD    DECIMAL(1),
    CURRENCY       CHAR(5),
    ROUTE          DECIMAL(5),
    INDICTOR       CHAR(1),
    ACCOUNT_NUMBER CHAR(40),
    C_DIGIT        DECIMAL(2),
    TMSTAMP        TIMESTAMP(6)
);

