create table EXPLAIN_PREDICATE
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
    PREDICATE_ID      INTEGER      not null,
    HOW_APPLIED       CHAR(10)     not null,
    WHEN_EVALUATED    CHAR(3)      not null,
    RELOP_TYPE        CHAR(2)      not null,
    SUBQUERY          CHAR(1)      not null,
    FILTER_FACTOR     DOUBLE       not null,
    PREDICATE_TEXT    CLOB(2097152),
    RANGE_NUM         INTEGER,
    INDEX_COLSEQ      INTEGER      not null,
    foreign key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO) references EXPLAIN_STATEMENT
        on delete cascade
);

create unique index PRD_I2
    on EXPLAIN_PREDICATE (EXPLAIN_TIME, EXPLAIN_LEVEL, STMTNO, SECTNO, OPERATOR_ID);

