create table EXPLAIN_STREAM
(
    EXPLAIN_REQUESTER VARCHAR(128) not null,
    EXPLAIN_TIME      TIMESTAMP(6) not null,
    SOURCE_NAME       VARCHAR(128) not null,
    SOURCE_SCHEMA     VARCHAR(128) not null,
    SOURCE_VERSION    VARCHAR(64)  not null,
    EXPLAIN_LEVEL     CHAR(1)      not null,
    STMTNO            INTEGER      not null,
    SECTNO            INTEGER      not null,
    STREAM_ID         INTEGER      not null,
    SOURCE_TYPE       CHAR(1)      not null,
    SOURCE_ID         INTEGER      not null,
    TARGET_TYPE       CHAR(1)      not null,
    TARGET_ID         INTEGER      not null,
    OBJECT_SCHEMA     VARCHAR(128),
    OBJECT_NAME       VARCHAR(128),
    STREAM_COUNT      DOUBLE       not null,
    COLUMN_COUNT      SMALLINT     not null,
    PREDICATE_ID      INTEGER      not null,
    COLUMN_NAMES      CLOB(2097152),
    PMID              SMALLINT     not null,
    SINGLE_NODE       CHAR(5),
    PARTITION_COLUMNS CLOB(2097152),
    SEQUENCE_SIZES    CLOB(2097152),
    foreign key (EXPLAIN_REQUESTER, EXPLAIN_TIME, SOURCE_NAME, SOURCE_SCHEMA, SOURCE_VERSION, EXPLAIN_LEVEL, STMTNO,
                 SECTNO) references EXPLAIN_STATEMENT
        on delete cascade
);

create unique index STM_I2
    on EXPLAIN_STREAM (EXPLAIN_TIME, EXPLAIN_LEVEL, STMTNO, SECTNO);

