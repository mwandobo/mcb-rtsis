create table TMP_RISK_1
(
    C_DIGIT               SMALLINT,
    ACCOUNT_CD            SMALLINT,
    ID_CURRENCY           INTEGER,
    CRRAT                 INTEGER,
    ACC_UNIT              INTEGER,
    ID_PRODUCT            INTEGER,
    CKORY                 INTEGER,
    CUST_ID               INTEGER,
    EUR_BALANCE           DECIMAL(15, 2),
    EUR_OVERDUE           DECIMAL(15, 2),
    NRM_BAL               DECIMAL(15, 2),
    LOAN_ACC_LIMIT        DECIMAL(15, 2),
    LOAN_OVERDUE_DAYS     VARCHAR(40),
    ACC_OPEN_DT           DATE,
    LNS_LMT_RENEWAL_STAMP TIMESTAMP(6),
    CURR_RATE             CHAR(10),
    ACCOUNT               CHAR(40),
    SURNAME               CHAR(70),
    LOAN_STATUS           VARCHAR(22),
    DESCRIPTION           VARCHAR(40),
    CRRAT_DESC            VARCHAR(40),
    CKORY_DESC            VARCHAR(40)
);

