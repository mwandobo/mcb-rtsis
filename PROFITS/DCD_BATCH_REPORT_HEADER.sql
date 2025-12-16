create table DCD_BATCH_REPORT_HEADER
(
    SN                 SMALLINT not null,
    EXEC_DATE          DATE     not null,
    BATCH_ID           CHAR(5)  not null,
    DCD_REPOER_REP_ID  DECIMAL(12),
    DCD_REPOER_PROJECT DECIMAL(12),
    TIMESTAMP          TIMESTAMP(6),
    REPL_PARAMS        VARCHAR(2048),
    LIST_PARAMS        VARCHAR(2048),
    constraint IXU_DCD_050
        primary key (SN, EXEC_DATE, BATCH_ID)
);

