create table EXPLAIN_ARGUMENT
(
    EXPLAIN_REQUESTER   VARCHAR(128) not null,
    EXPLAIN_TIME        TIMESTAMP(6) not null,
    SOURCE_NAME         VARCHAR(128) not null,
    SOURCE_SCHEMA       VARCHAR(128) not null,
    SOURCE_VERSION      VARCHAR(64)  not null,
    EXPLAIN_LEVEL       CHAR(1)      not null,
    STMTNO              INTEGER      not null,
    SECTNO              INTEGER      not null,
    OPERATOR_ID         INTEGER      not null,
    ARGUMENT_TYPE       CHAR(8)      not null,
    ARGUMENT_VALUE      VARCHAR(1024),
    LONG_ARGUMENT_VALUE CLOB(2097152),
    foreign key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO) references EXPLAIN_STATEMENT
        on delete cascade
);

create unique index ARG_I2
    on EXPLAIN_ARGUMENT (EXPLAIN_TIME, EXPLAIN_LEVEL, STMTNO, SECTNO, OPERATOR_ID);

