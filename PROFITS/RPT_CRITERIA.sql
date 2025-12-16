create table RPT_CRITERIA
(
    FK_REPORT_ID            INTEGER            not null,
    ID                      INTEGER            not null,
    LABEL                   VARCHAR(50)        not null,
    TYPE                    SMALLINT default 0 not null,
    VALUE_FORMAT            VARCHAR(50),
    DESCRIPTION             VARCHAR(200)       not null,
    DEFAULT_VALUE           VARCHAR(2000)      not null,
    ANALYSIS                VARCHAR(2000),
    BREAKTYPE               SMALLINT default 0 not null,
    VALUETYPE               SMALLINT default 0,
    CRITERIA_SOURCE         SMALLINT default 0 not null,
    MULTIPLE_SELECTION_TYPE SMALLINT default 0 not null,
    LINE_OPERATION          SMALLINT default 0 not null,
    constraint RPT_CRITERIA_PK
        primary key (FK_REPORT_ID, ID)
);

