create table BENCHMARK_USE
(
    UNIQUE_KEY      DECIMAL(10) not null
        constraint PK_BNCH_7
            primary key,
    PRFT_SYSTEM     SMALLINT,
    TRX_CODE        INTEGER,
    SMALL_AMOUNT    DECIMAL(15, 2),
    LARGE_AMOUNT    DECIMAL(15, 2),
    PROFITS_ACCOUNT CHAR(40)
);

