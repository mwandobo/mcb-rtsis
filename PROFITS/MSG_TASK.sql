create table MSG_TASK
(
    ID                   DECIMAL(12)           not null
        constraint IXM_TSK_001
            primary key,
    FK_SQL_REP_ID        DECIMAL(12)           not null
        constraint FK_SQLRP3
            references MSG_SQL_REPOSITORY,
    DESCRIPTION          VARCHAR(255)          not null,
    FK_PRIORITY          SMALLINT              not null
        constraint FK_PRRT1
            references MSG_PRIORITY,
    APPROVAL_FLG         SMALLINT    default 0,
    STATUS               SMALLINT    default 0,
    STARTING_DATE        TIMESTAMP(6)          not null,
    ENDING_DATE          TIMESTAMP(6)          not null,
    DAYS_FREQUENCY       SMALLINT              not null,
    TIME_FREQUENCY       TIME                  not null,
    SUBJECT              VARCHAR(78) default '',
    BANK_ID              SMALLINT    default 1 not null,
    RUN_TYPE             SMALLINT    default 0,
    TRYING_TO_SEND       SMALLINT    default 1 not null,
    PAUSE_BETWEEN_FAILED TIME,
    ANALYSIS             VARCHAR(2000),
    IMPORTED             TIMESTAMP(6)
);

