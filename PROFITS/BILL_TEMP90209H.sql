create table BILL_TEMP90209H
(
    SUM_AMOUNT  DECIMAL(15, 2),
    SUM_COUNT   INTEGER,
    BISS_CODE   INTEGER not null,
    BISS_CDIGIT SMALLINT,
    CUST_ID     INTEGER not null,
    C_DIGIT     SMALLINT,
    UNIT_CODE   INTEGER,
    constraint BILL_TEMP90209H
        primary key (CUST_ID, BISS_CODE)
);

