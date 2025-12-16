create table BENCHMARK_ACTION
(
    UNIQUE_KEY    DECIMAL(10) not null
        constraint PK_BNCH_70
            primary key,
    RECORD_SYSTEM SMALLINT,
    RECORD_TYPE   CHAR(20),
    RECORD_ID     DECIMAL(10),
    RECORD_ACTION CHAR(20)
);

