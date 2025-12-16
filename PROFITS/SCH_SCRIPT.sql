create table SCH_SCRIPT
(
    ID                   VARCHAR(40)        not null
        constraint SCH_SCRIPT_PK
            primary key,
    LABEL                VARCHAR(50)        not null
        unique,
    DESCRIPTION          VARCHAR(2000)      not null,
    LAST_USER            VARCHAR(20)        not null,
    LAST_UPDATED         TIMESTAMP(6)       not null,
    ROOT                 VARCHAR(40)        not null,
    DELETED              SMALLINT default 0 not null,
    EXECUTION_TYPE       SMALLINT default 3 not null,
    CONCURRENT_EXECUTION SMALLINT default 0 not null
);

