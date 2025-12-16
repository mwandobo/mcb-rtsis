create table MSG_LOG_MSGDELIVERY
(
    ID           DECIMAL(12)           not null
        constraint IXM_LMD_001
            primary key,
    FK_OUTBOX_ID DECIMAL(12)           not null,
    TIME_STAMP   TIMESTAMP(6),
    LOG_MESSAGE  VARCHAR(2000),
    KEYDATETIME  TIMESTAMP(6),
    FK_TASK_ID   DECIMAL(12) default 0,
    ERROR_FLAG   SMALLINT    default 0 not null
);

create unique index IXM_LMD_002
    on MSG_LOG_MSGDELIVERY (TIME_STAMP);

create unique index IXM_LMD_003
    on MSG_LOG_MSGDELIVERY (FK_TASK_ID);

create unique index IXM_LMD_004
    on MSG_LOG_MSGDELIVERY (KEYDATETIME);

create unique index IXM_LMD_005
    on MSG_LOG_MSGDELIVERY (FK_TASK_ID, KEYDATETIME);

