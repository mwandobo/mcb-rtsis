create table SCH_EMBEDDED_SCRIPTS_BCK
(
    ID           VARCHAR(40)   not null
        constraint SCH_EMBEDDED_SCRIPTS_PK
            primary key,
    SCRIPT_TYPE  INTEGER       not null,
    CMD_TYPE     INTEGER       not null,
    SQL_TYPE     INTEGER       not null,
    SCRIPT       CLOB(1048576) not null,
    CREATED_BY   VARCHAR(255)  not null,
    CREATED_DATE TIMESTAMP(6)  not null,
    UPDATED_BY   VARCHAR(255),
    UPDATED_DATE TIMESTAMP(6)
);

