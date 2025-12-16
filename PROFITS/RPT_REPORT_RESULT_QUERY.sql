create table RPT_REPORT_RESULT_QUERY
(
    FK_REPORT_RESULT_ID BIGINT            not null,
    FK_QUERY_ID         INTEGER           not null,
    QUERY               CLOB(1048576),
    FK_DATABASE_ID      INTEGER,
    QUERY_ROWS          INTEGER,
    FK_REPORT_ID        INTEGER,
    QUERY_SN            INTEGER default 1 not null,
    constraint RPT_REPORT_RESULT_QUERY_PK
        primary key (FK_REPORT_RESULT_ID, FK_QUERY_ID, QUERY_SN)
);

