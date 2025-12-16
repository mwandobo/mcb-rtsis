create table TMP_EOM_DEPOSIT_1
(
    CUST_ID            INTEGER,
    ACCOUNT_NUMBER     CHAR(40),
    ACCOUNT_CD         SMALLINT,
    DEP_ACC_NUMBER     DECIMAL(11),
    EURO_BOOK_BAL      DECIMAL(15, 2),
    EXPIRY_DATE        DATE,
    ID_PRODUCT         INTEGER,
    FIRST_NAME         CHAR(20),
    SURNAME            CHAR(70),
    CUST_TYPE          CHAR(1),
    TAX_ID             CHAR(20),
    RATE               DECIMAL(8, 4),
    HITS               VARCHAR(10),
    ACCOUNT_TYPE       VARCHAR(2),
    REP_ACCOUNT_NUMBER VARCHAR(40),
    REP_RATE           VARCHAR(40),
    REP_MATURITY_DATE  VARCHAR(10),
    FKGH_CATEGORY      CHAR(5),
    FKGD_CATEGORY      INTEGER
);

create unique index IDX_CUSTID_TMP_EOM_DEP1
    on TMP_EOM_DEPOSIT_1 (CUST_ID);

