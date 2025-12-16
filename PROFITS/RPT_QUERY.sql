create table RPT_QUERY
(
    ID              INTEGER            not null,
    FK_REPORT_ID    INTEGER            not null,
    POSITION        INTEGER            not null,
    ACTUAL_QUERY    CLOB(1048576),
    NAME            VARCHAR(50),
    DESCRIPTION     VARCHAR(200)       not null,
    CREATED         TIMESTAMP(6)       not null,
    CREATED_BY      VARCHAR(50)        not null,
    UPDATED         TIMESTAMP(6)       not null,
    UPDATED_BY      VARCHAR(50)        not null,
    DELETED         SMALLINT default 0 not null,
    STATUS          SMALLINT default 0 not null,
    FK_DATA_BASE    INTEGER  default 0,
    ANALYSIS        VARCHAR(2000),
    UPDATE_FORMULAS SMALLINT default 0 not null,
    constraint RPT_QUERY_PK
        primary key (ID, FK_REPORT_ID)
);

