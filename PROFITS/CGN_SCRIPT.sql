create table CGN_SCRIPT
(
    SCRIPT_SN         DECIMAL(10)  not null
        constraint PK_SQL_SCRIPT
            primary key,
    SCRIPT_NAME       VARCHAR(50),
    SCRIPT_ONLY       CHAR(1),
    TABLE_SN          DECIMAL(10)  not null,
    SCRIPT_TABLE_TYPE CHAR(1)      not null,
    DB_VIEW_NAME      VARCHAR(25),
    DB_VIEW_SCRIPT    VARCHAR(4000),
    TMSTAMP           TIMESTAMP(6) not null
);

