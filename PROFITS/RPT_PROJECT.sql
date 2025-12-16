create table RPT_PROJECT
(
    ID           INTEGER            not null
        constraint RPT_PROJECT_PK
            primary key,
    FK_DATA_BASE INTEGER  default 1 not null,
    NAME         VARCHAR(50)        not null,
    CREATED      TIMESTAMP(6)       not null,
    CREATED_BY   VARCHAR(50)        not null,
    UPDATED      TIMESTAMP(6)       not null,
    UPDATED_BY   VARCHAR(50)        not null,
    DELETED      SMALLINT default 0 not null,
    STATUS       SMALLINT default 0 not null,
    DESCRIPTION  VARCHAR(200),
    ANALYSIS     VARCHAR(2000),
    LOCKED       SMALLINT default 0 not null
);

