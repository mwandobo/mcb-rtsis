create table CLC_PAY_PROMISES
(
    CASE_ID         CHAR(40)          not null,
    CUST_ID         INTEGER           not null,
    PAY_NUMBER      INTEGER           not null,
    PAY_DT          DATE,
    PAY_AMN         DECIMAL(18, 2),
    PAY_COMMENTS    VARCHAR(80),
    PAID_AMN        DECIMAL(18, 2),
    PAID_DT         DATE,
    CREATE_UNIT     INTEGER,
    CREATE_DATE     DATE,
    CREATE_USER     CHAR(8),
    CREATE_TMSTAMP  TIMESTAMP(6),
    UPDATE_UNIT     INTEGER,
    UPDATE_DATE     DATE,
    UPDATE_USER     CHAR(8),
    UPDATE_TMSTAMP  TIMESTAMP(6),
    TABLE_STATUS    CHAR(1),
    PAY_RESULT_STS  CHAR(1),
    PAY_PERCENT     DECIMAL(12, 6),
    PAY_PERCENT_AMN DECIMAL(18, 2),
    PAY_TOLERANCE   SMALLINT,
    PAY_PROMISE_ID  INTEGER default 0 not null,
    constraint CLC_COLLECT_PK_6
        primary key (CUST_ID, CASE_ID, PAY_PROMISE_ID, PAY_NUMBER)
);

