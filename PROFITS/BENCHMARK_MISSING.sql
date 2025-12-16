create table BENCHMARK_MISSING
(
    PRFT_SYSTEM    SMALLINT not null,
    PRODUCT_ID     INTEGER  not null,
    CURRENCY_ID    INTEGER  not null,
    PRODUCT_DESC   CHAR(40),
    CURRENCY_DESC  CHAR(40),
    ACCOUNT_EXISTS CHAR(10),
    constraint PK_BNCH_8
        primary key (CURRENCY_ID, PRODUCT_ID, PRFT_SYSTEM)
);

