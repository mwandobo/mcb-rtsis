create table RPT_QUERYCOLUMN
(
    FK_REPORT_ID  INTEGER            not null,
    FK_QUERY_ID   INTEGER            not null,
    QUERY_COLUMN  INTEGER            not null,
    LABEL         VARCHAR(30)        not null,
    SHEET         INTEGER  default 0,
    COLUMN_NUMBER INTEGER  default 0,
    ROW_NUMBER    INTEGER  default 0,
    DATA_TABLE    VARCHAR(30),
    DATA_COLUMN   VARCHAR(30),
    DATA_TYPE     SMALLINT           not null,
    BREAKTYPE     SMALLINT default 0 not null,
    DATA_FORMAT   VARCHAR(50),
    ANALYSIS      VARCHAR(2000),
    constraint RPT_QUERYCOLUMN_PK
        primary key (FK_REPORT_ID, FK_QUERY_ID, QUERY_COLUMN)
);

