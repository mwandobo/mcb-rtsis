create table MSG_LOG_SQLRUNNER
(
    ID                  DECIMAL(12)        not null
        constraint IXM_LSR_001
            primary key,
    FK_TASK_ID          DECIMAL(12)        not null,
    START_SQL_EXECUTION TIMESTAMP(6),
    END_SQL_EXECUTION   TIMESTAMP(6),
    LINES_AFFECTED      DECIMAL(12),
    LOG_MESSAGE         VARCHAR(2000),
    KEYDATETIME         TIMESTAMP(6),
    ERROR_FLAG          SMALLINT default 0 not null
);

