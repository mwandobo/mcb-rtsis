create table HSQL_BATCH_UPDATE
(
    TIMESTAMP     TIMESTAMP(6) not null
        constraint IXU_HSQ_000
            primary key,
    SN            INTEGER,
    TRX_DATE      DATE,
    PROCESS_START TIMESTAMP(6),
    PROCESS_END   TIMESTAMP(6),
    BATCH_ID      VARCHAR(5),
    RESULT        VARCHAR(50),
    SQL_STATEMENT VARCHAR(4000)
);

