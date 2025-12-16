create table SCH_SCRIPT_SSN
(
    SESSION_ID           TIMESTAMP(6)       not null,
    ID                   VARCHAR(40)        not null,
    LABEL                VARCHAR(50)        not null,
    DESCRIPTION          VARCHAR(2000)      not null,
    LAST_USER            VARCHAR(20)        not null,
    LAST_UPDATED         TIMESTAMP(6)       not null,
    ROOT                 VARCHAR(40)        not null,
    DELETED              SMALLINT default 0 not null,
    EXECUTION_TYPE       SMALLINT default 3 not null,
    CONCURRENT_EXECUTION SMALLINT default 0 not null
);

