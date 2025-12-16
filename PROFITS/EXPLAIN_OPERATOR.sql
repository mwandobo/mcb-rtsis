create table EXPLAIN_OPERATOR
(
    EXPLAIN_REQUESTER VARCHAR(128) not null,
    EXPLAIN_TIME      TIMESTAMP(6) not null,
    SOURCE_NAME       VARCHAR(128) not null,
    SOURCE_SCHEMA     VARCHAR(128) not null,
    SOURCE_VERSION    VARCHAR(64)  not null,
    EXPLAIN_LEVEL     CHAR(1)      not null,
    STMTNO            INTEGER      not null,
    SECTNO            INTEGER      not null,
    OPERATOR_ID       INTEGER      not null,
    OPERATOR_TYPE     CHAR(6)      not null,
    TOTAL_COST        DOUBLE       not null,
    IO_COST           DOUBLE       not null,
    CPU_COST          DOUBLE       not null,
    FIRST_ROW_COST    DOUBLE       not null,
    RE_TOTAL_COST     DOUBLE       not null,
    RE_IO_COST        DOUBLE       not null,
    RE_CPU_COST       DOUBLE       not null,
    COMM_COST         DOUBLE       not null,
    FIRST_COMM_COST   DOUBLE       not null,
    BUFFERS           DOUBLE       not null,
    REMOTE_TOTAL_COST DOUBLE       not null,
    REMOTE_COMM_COST  DOUBLE       not null,
    primary key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO, OPERATOR_ID),
    foreign key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO) references EXPLAIN_STATEMENT
        on delete cascade
);

create unique index OPR_I2
    on EXPLAIN_OPERATOR (EXPLAIN_TIME, EXPLAIN_LEVEL, STMTNO, SECTNO, OPERATOR_ID);

