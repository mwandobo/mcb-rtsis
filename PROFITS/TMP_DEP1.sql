create table TMP_DEP1
(
    C_DIGIT                SMALLINT,
    ACCOUNT_CD             SMALLINT,
    PRODUCT_ID             INTEGER,
    MOVEMENT_CURRENCY      INTEGER,
    CUST_ID                INTEGER,
    EURO_BOOK_BAL_INTEREST DECIMAL(15, 2),
    EURO_ACCR_CR_INTEREST  DECIMAL(15, 2),
    EURO_INTEREST_AMOUNT   DECIMAL(15, 2),
    EURO_SOURCE_AMOUNT     DECIMAL(15, 2),
    GROUP_RANGE_VAL        DECIMAL(15, 2),
    EXPIRY_DATE            DATE,
    CUST_TYPE              CHAR(2),
    SHORT_DESCR            CHAR(5),
    FIRST_NAME             CHAR(20),
    ACCOUNT_NUMBER         CHAR(40),
    SURNAME                CHAR(70),
    GROUP_RANGE            VARCHAR(10),
    DESCRIPTION            VARCHAR(40)
);

