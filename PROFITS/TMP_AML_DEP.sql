create table TMP_AML_DEP
(
    MONOTORING_UNIT      INTEGER,
    SOURCE_CURRENCY      INTEGER,
    CUST_ID              INTEGER,
    FIXING               DECIMAL(12, 6),
    EUR_SUM_ENTRY_AMOUNT DECIMAL(15, 2),
    SUM_ENTRY_AMOUNT     DECIMAL(15, 2),
    FIRST_NAME           CHAR(20),
    SURNAME              CHAR(70),
    CUST_TYPE            VARCHAR(12)
);

