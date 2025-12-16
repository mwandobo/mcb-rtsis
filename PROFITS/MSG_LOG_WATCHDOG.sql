create table MSG_LOG_WATCHDOG
(
    ID          BIGINT             not null
        primary key,
    FK_TASK_ID  BIGINT,
    FK_OUTBOXID BIGINT,
    LOG_MESSAGE VARCHAR(2000),
    CREATED     TIMESTAMP(6),
    ERROR_FLAG  SMALLINT default 0 not null
);

create unique index WATCHDOGLOGOUTBOXID_IDX
    on MSG_LOG_WATCHDOG (FK_OUTBOXID);

create unique index WATCHDOGLOGTASKID_IDX
    on MSG_LOG_WATCHDOG (FK_TASK_ID);

