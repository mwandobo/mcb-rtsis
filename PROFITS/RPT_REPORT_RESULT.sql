create table RPT_REPORT_RESULT
(
    START_TIME           TIMESTAMP(6)       not null,
    END_TIME             TIMESTAMP(6)       not null,
    REPORT_USER          VARCHAR(50)        not null,
    FK_REPORT_ID         INTEGER            not null,
    FK_PROJECT_ID        INTEGER            not null,
    FK_DATABASE_ID       INTEGER            not null,
    FK_LANGUAGE_ID       INTEGER            not null,
    FK_TEMPLATE_ID       INTEGER            not null,
    STATUS               SMALLINT default 0 not null,
    LOGMESSAGE           VARCHAR(4000),
    ID                   BIGINT   default 0 not null
        constraint RPT_RESULT_PK
            primary key,
    EXECUTION_TYPE       SMALLINT default 1,
    CRITERIA_HASH        CHAR(24),
    CRITERIA_CONCAT      VARCHAR(4000),
    FREE_TEXT            VARCHAR(200),
    REPORT_RESULT_STATUS SMALLINT default 1
);

create unique index IXN_RPT_003
    on RPT_REPORT_RESULT (FK_REPORT_ID, STATUS, CRITERIA_HASH);

