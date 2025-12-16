create table TMP_LIMITS
(
    CHECK_DIGIT            SMALLINT,
    SUB_SYSTEM             SMALLINT,
    PROFITS_ACCOUNT_CD     SMALLINT,
    TRX_UNIT               INTEGER,
    TRX_SN                 INTEGER,
    TRX_CODE               INTEGER,
    CUSTOMER_ID            INTEGER,
    ACCOUNT_LIMIT          DECIMAL(15, 2),
    TRX_DATE               DATE,
    CUSTOMER_TYPE          CHAR(1),
    TRX_USR                CHAR(8),
    FIRST_NAME             CHAR(20),
    PROFITS_ACCOUNT_NUMBER CHAR(40),
    SURNAME                CHAR(70),
    TRX_CODE_DESC          VARCHAR(40)
);

