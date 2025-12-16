create table TMP_AML_DEP2
(
    C_DIGIT              SMALLINT,
    SOURCE_CURRENCY      INTEGER,
    MONOTORING_UNIT      INTEGER,
    CUST_ID              INTEGER,
    FIXING               DECIMAL(12, 6),
    SUM_ENTRY_AMOUNT     DECIMAL(15, 2),
    EUR_SUM_ENTRY_AMOUNT DECIMAL(15, 2),
    TRX_DATE             DATE,
    FIRST_NAME           CHAR(20),
    SURNAME              CHAR(70),
    CUST_TYPE            VARCHAR(12)
);

