create table PAT_EXECUTION_VALUE_LOG
(
    ID                         DECIMAL(10) not null
        constraint PATVLGPK
            primary key,
    NAME                       CHAR(32)    not null,
    STATIC_DATA                CHAR(100),
    ACTION_ID                  DECIMAL(10),
    ID_INSTRUCTION             DECIMAL(10),
    FK_PAT_LOG_TS_EXECUTION_ID DECIMAL(10),
    PHASE                      CHAR(1),
    TYPE                       CHAR(1)
);

create unique index PATVLGI1
    on PAT_EXECUTION_VALUE_LOG (FK_PAT_LOG_TS_EXECUTION_ID);

