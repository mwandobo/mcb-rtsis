create table DEP_CUST_EXTRA
(
    CUST_ID        INTEGER      not null,
    ACCOUNT_NUMBER CHAR(40)     not null,
    PRFT_SYSTEM    SMALLINT     not null,
    PRINT_PATH     CHAR(254)    not null,
    TMSTAMP        TIMESTAMP(6) not null
);

