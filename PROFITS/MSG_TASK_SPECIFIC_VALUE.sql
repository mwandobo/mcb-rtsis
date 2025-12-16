create table MSG_TASK_SPECIFIC_VALUE
(
    FK_TASK_ID      DECIMAL(12) not null,
    FK_FIELD_MAP_ID SMALLINT    not null,
    FK_SQL_REP_ID   DECIMAL(12) not null,
    SPECIFIC_VALUE  VARCHAR(200),
    constraint IXM_TSV_001
        primary key (FK_TASK_ID, FK_FIELD_MAP_ID, FK_SQL_REP_ID)
);

