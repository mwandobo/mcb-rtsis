create table PAT_EXECUTION_LOG
(
    TS_EXECUTION_ID           DECIMAL(10) not null
        constraint PATEXPK1
            primary key,
    INDEX1                    INTEGER     not null,
    EXECUTION_START_TIMESTAMP TIMESTAMP(6),
    STATUS                    CHAR(3)     not null,
    RUN_COUNT                 INTEGER     not null,
    EXECUTION_END_TIMESTAMP   TIMESTAMP(6),
    LAST_INSTRUCTION_SEQ_ID   DECIMAL(10) not null,
    FK_PAT_RUN_SCENID         INTEGER,
    FK_PAT_TRANSACTID         INTEGER,
    PROFILE_ID                CHAR(10),
    BANK_NAME                 CHAR(10),
    DB_CONNECT_STRING         CHAR(240),
    TESTER_NAME               CHAR(60),
    CLIENT_TYPE               CHAR(1),
    LANGUAGE_CODE             CHAR(2),
    DISABLED                  CHAR(1)
);

create unique index PATEXI01
    on PAT_EXECUTION_LOG (FK_PAT_TRANSACTID);

create unique index PATEXI02
    on PAT_EXECUTION_LOG (FK_PAT_RUN_SCENID);

