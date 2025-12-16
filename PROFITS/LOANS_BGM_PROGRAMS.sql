create table LOANS_BGM_PROGRAMS
(
    BGM_PROGRAM    VARCHAR(20) not null
        constraint BGMPROGRAMMAIN
            primary key,
    BGM_PROG_ID    INTEGER     not null,
    BGM_PROG_DESCR VARCHAR(100),
    BGM_BATCH_FLAG CHAR(1),
    TMSTAMP        TIMESTAMP(6),
    ENTRY_STATUS   CHAR(1),
    PROCESSING_STS CHAR(1)
);

create unique index BGMPROGRAMSECONDARY
    on LOANS_BGM_PROGRAMS (BGM_PROG_ID);

