create table EXT_TASK_EXECUTE
(
    BATCH_ID          CHAR(5)      not null,
    EXT_TASK_ID       DECIMAL(10)  not null,
    BATCH_TRX_DATE    DATE         not null,
    EXT_TASK_TMSTAMP  TIMESTAMP(6) not null,
    EXT_TASK_DATA_SQL VARCHAR(4000),
    EXT_TASK_RESULT   CHAR(50),
    constraint PK_EXTTSK3
        primary key (EXT_TASK_TMSTAMP, BATCH_TRX_DATE, EXT_TASK_ID, BATCH_ID)
);

