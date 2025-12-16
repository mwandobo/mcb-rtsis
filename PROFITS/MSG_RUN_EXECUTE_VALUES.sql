create table MSG_RUN_EXECUTE_VALUES
(
    FK_TASK_ID      DECIMAL(12) not null,
    FK_FIELD_MAP_ID SMALLINT    not null,
    FK_SQL_REP_ID   DECIMAL(12) not null,
    LAST_RECORD     VARCHAR(200),
    constraint IXM_REV_001
        primary key (FK_TASK_ID, FK_FIELD_MAP_ID, FK_SQL_REP_ID),
    constraint FK_SFMP2
        foreign key (FK_FIELD_MAP_ID, FK_SQL_REP_ID) references MSG_SQL_FIELD_MAPPING
);

