create table TMP_TEMP_RESULTS
(
    ACCOUNT_NUMBER DECIMAL(11) not null
        constraint IXU_REP_160
            primary key,
    CODE           INTEGER,
    CUST_ID        INTEGER,
    SUM_DEBIT      DECIMAL(15, 2),
    SUM_CREDIT     DECIMAL(15, 2),
    SHORT_DESCR    CHAR(5),
    FIRST_NAME     CHAR(20),
    SURNAME        CHAR(70),
    DESCRIPTION    VARCHAR(40)
);

