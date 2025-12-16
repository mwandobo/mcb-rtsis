create table DEP_BATCH_EXECUTION
(
    PROGRAM_ID        CHAR(40) not null
        constraint PK_DEP_BATCH_EXECUTION
            primary key,
    LAST_EXECUTION_DT DATE,
    TMSTAMP           DATE
);

