create table EXT_TASK_ASK_EXE
(
    TRX_TASK_ID       DECIMAL(10)  not null,
    TRX_ACC_NUMBER    CHAR(40)     not null,
    TRX_CUST_ID       INTEGER      not null,
    TRX_DATE          DATE         not null,
    TRX_TMSTAMP       TIMESTAMP(6) not null,
    TRX_COUNTER       DECIMAL(12)  not null,
    EXT_TASK_RESULT   CHAR(50),
    EXT_TASK_DATA_SQL VARCHAR(4000),
    constraint PK_EXTTSK7
        primary key (TRX_COUNTER, TRX_TMSTAMP, TRX_DATE, TRX_CUST_ID, TRX_ACC_NUMBER, TRX_TASK_ID)
);

