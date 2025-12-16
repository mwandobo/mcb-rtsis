create table TMP_COL_YIELD_LIMIT
(
    ACCOUNT_NUMBER CHAR(40) not null,
    PRFT_SYSTEM    SMALLINT not null,
    YIELD_LIMIT_A  DECIMAL(15, 2),
    YIELD_LIMIT_B  DECIMAL(15, 2),
    AGR_NUMBER     CHAR(40),
    constraint IXU_REP_113
        primary key (ACCOUNT_NUMBER, PRFT_SYSTEM)
);

