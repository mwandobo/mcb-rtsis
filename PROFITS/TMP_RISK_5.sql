create table TMP_RISK_5
(
    C_DIGIT                SMALLINT,
    ACCOUNT_CD             SMALLINT,
    OVERDUE_CLASSIFICATION SMALLINT,
    ACC_UNIT               INTEGER,
    ID_PRODUCT             INTEGER,
    CUST_ID                INTEGER,
    DEP_NRM_BAL            DECIMAL(15, 2),
    DEP_OVER_DUE_BAL       DECIMAL(15, 2),
    EUR_DEP_BOOK_BAL       DECIMAL(15, 2),
    LOAN_ACC_LIMIT         DECIMAL(15, 2),
    EXPIRY_DATE            DATE,
    ACC_OPEN_DT            DATE,
    LOAN_STATUS            CHAR(1),
    ACCOUNT                CHAR(40),
    SURNAME                CHAR(70),
    DESCRIPTION            VARCHAR(40)
);

