create table MSG_LOG_MSGPREPARE
(
    ID              DECIMAL(12)           not null
        constraint IXM_LMP_001
            primary key,
    FK_TASK_ID      DECIMAL(12)           not null,
    FK_CHANNEL_ID   SMALLINT,
    TIME_STAMP      TIMESTAMP(6),
    LOG_MESSAGE     VARCHAR(2000),
    MESSAGESCREATED DECIMAL(10) default 0,
    OUTBOX_ID_FROM  DECIMAL(12) default 0,
    OUTBOX_ID_TO    DECIMAL(12) default 0,
    DURATION        TIME,
    RUN_TYPE        SMALLINT    default 0,
    FK_RECIPIENT_ID DECIMAL(12) default 0 not null,
    ERROR_FLAG      SMALLINT    default 0 not null,
    KEYDATETIME     TIMESTAMP(6),
    PRINT_FLG       CHAR(1)
);

create unique index IXM_LMP_002
    on MSG_LOG_MSGPREPARE (FK_TASK_ID);

create unique index IXM_LMP_003
    on MSG_LOG_MSGPREPARE (KEYDATETIME);

create unique index IXM_LMP_004
    on MSG_LOG_MSGPREPARE (FK_TASK_ID, KEYDATETIME);

