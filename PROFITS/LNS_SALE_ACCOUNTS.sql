create table LNS_SALE_ACCOUNTS
(
    ACCOUNT_NUMBER CHAR(40) not null
        constraint PK_SALE_ACC
            primary key,
    ID_JUSTIFIC    INTEGER,
    PROCESS_STS    CHAR(1),
    TMSTAMP        TIMESTAMP(6)
);

