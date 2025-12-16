create table CUST_PROBLEM_DETAIL
(
    ENTRY_SN        INTEGER      not null,
    CUST_ID         INTEGER      not null,
    TRX_DATE        DATE         not null,
    TMSTAMP         TIMESTAMP(6) not null,
    CUST_CD         SMALLINT,
    ACCOUNT_CD      SMALLINT,
    PRFT_SYSTEM     SMALLINT,
    CURRENCY_ID     INTEGER,
    UNIT_CODE       INTEGER,
    PRODUCT_ID      INTEGER,
    CUST_TYPE       CHAR(1),
    PROBLEM_RECORD  CHAR(1),
    ENTRY_TYPE      CHAR(2),
    CURRENCY_DESC   CHAR(5),
    CUST_FIRST_NAME CHAR(20),
    PRODUCT_DESC    CHAR(40),
    UNIT_NAME       CHAR(40),
    ACCOUNT_NUMBER  CHAR(40),
    CUST_SURNAME    CHAR(70),
    PROBLEM_DESCR   VARCHAR(500),
    constraint IXU_CIS_170
        primary key (ENTRY_SN, CUST_ID, TRX_DATE, TMSTAMP)
);

