create table TMP_RISK_4
(
    C_DIGIT            SMALLINT,
    ACCOUNT_CD         SMALLINT,
    ACC_UNIT           INTEGER,
    CUST_ACTIVITY      INTEGER,
    ID_PRODUCT         INTEGER,
    CUST_ID            INTEGER,
    EUR_BALANCE        DECIMAL(15, 2),
    EUR_OVERDUE        DECIMAL(15, 2),
    LOAN_OVERDUE_DAYS  DECIMAL(15, 2),
    NRM_BAL            DECIMAL(15, 2),
    ACC_OPEN_DT        DATE,
    LOAN_STATUS        CHAR(1),
    ACCOUNT            CHAR(40),
    SURNAME            CHAR(70),
    CUST_ACTIVITY_DESC VARCHAR(40),
    DESCRIPTION        VARCHAR(40)
);

