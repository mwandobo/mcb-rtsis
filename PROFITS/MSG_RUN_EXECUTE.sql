create table MSG_RUN_EXECUTE
(
    FK_TASK_ID     DECIMAL(12)  not null
        constraint IXM_REX_001
            primary key,
    LAST_EXECUTION TIMESTAMP(6),
    NEXT_EXECUTION TIMESTAMP(6) not null
);

