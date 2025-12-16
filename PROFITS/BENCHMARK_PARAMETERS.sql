create table BENCHMARK_PARAMETERS
(
    BANK_CODE        SMALLINT not null
        constraint PK_BNCH_10
            primary key,
    ITERATION_NUMBER INTEGER,
    TERM_ADD_DAYS    SMALLINT,
    USE_ATM          CHAR(1),
    CREDIT_LOANS_SO  CHAR(1),
    LG_LOAN_PRODUCT  INTEGER
);

