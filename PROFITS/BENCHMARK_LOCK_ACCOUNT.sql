create table BENCHMARK_LOCK_ACCOUNT
(
    PRFT_SYSTEM    SMALLINT not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    ACCOUNT_CD     SMALLINT,
    CUST_ID        INTEGER,
    ENTRY_STATUS   CHAR(1),
    constraint IXU_BNC_003
        primary key (PRFT_SYSTEM, ACCOUNT_NUMBER)
);

