create table MSG_CHANNEL_EXECUTE
(
    FK_TASK_ID     DECIMAL(12)  not null,
    FK_CHANNEL_ID  SMALLINT     not null,
    LAST_EXECUTION TIMESTAMP(6),
    NEXT_EXECUTION TIMESTAMP(6) not null,
    constraint IXM_CEX_001
        primary key (FK_TASK_ID, FK_CHANNEL_ID)
);

