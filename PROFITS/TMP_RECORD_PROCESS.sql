create table TMP_RECORD_PROCESS
(
    PROGRAM_ID        CHAR(5)      not null,
    PROC_DATE         DATE         not null,
    TMSTAMP           TIMESTAMP(6) not null,
    EXECUTION_SN      SMALLINT,
    RECORDS_PROCESSED INTEGER,
    constraint PK_TMP_REC_PR
        primary key (TMSTAMP, PROC_DATE, PROGRAM_ID)
);

