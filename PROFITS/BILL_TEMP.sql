create table BILL_TEMP
(
    TIMESTAMP            TIMESTAMP(6) not null,
    NEW_BALANCE_AMOUNT   DECIMAL(15, 2),
    C_DIGIT              SMALLINT,
    SUM_BILL_AMOUNT      DECIMAL(15, 2),
    SUM_CR_AMOUNT        DECIMAL(15, 2),
    SUM_DB_AMOUNT        DECIMAL(15, 2),
    COUNT_BILLS          INTEGER,
    CUST_ID              INTEGER      not null,
    UNIT                 INTEGER      not null,
    PROCUCT_ID           INTEGER      not null,
    CURRENCY             INTEGER,
    COUNT_PREV_BILLS     INTEGER,
    SUM_CR_COUNT         INTEGER,
    SUM_DB_COUNT         INTEGER,
    PRODUCT_DESCRIPTION  CHAR(40),
    CURRENCY_DESCRIPTION CHAR(40),
    PREV_BALANCE         DECIMAL(15, 2),
    OVERDUE_FLAG         CHAR(1)
);

create unique index PK_BILL_TEMP
    on BILL_TEMP (OVERDUE_FLAG, UNIT, CUST_ID);

