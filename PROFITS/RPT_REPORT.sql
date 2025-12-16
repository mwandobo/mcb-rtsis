create table RPT_REPORT
(
    ID              INTEGER            not null
        constraint RPT_REPORT_PK
            primary key,
    FK_PROJECT_ID   INTEGER            not null,
    NAME            VARCHAR(50)        not null,
    DESCRIPTION     VARCHAR(200)       not null,
    CREATED         TIMESTAMP(6)       not null,
    CREATED_BY      VARCHAR(50)        not null,
    UPDATED         TIMESTAMP(6)       not null,
    UPDATED_BY      VARCHAR(50)        not null,
    DELETED         SMALLINT default 0 not null,
    STATUS          SMALLINT default 0 not null,
    ANALYSIS        VARCHAR(2000),
    PRIORITY_LEVEL  SMALLINT default 0 not null,
    FK_SUBSYSTEM_ID INTEGER  default 0 not null,
    EXECUTION_TYPE  SMALLINT default 7,
    VALID_FOR       TIME,
    HIDDEN_COMMENTS VARCHAR(2000)
);

