create table TMP_AML_DEP3
(
    CUST_TYPE        VARCHAR(12),
    MONOTORING_UNIT  INTEGER,
    CUST_ID          INTEGER  not null,
    SURNAME          CHAR(70) not null,
    FIRST_NAME       CHAR(20),
    ENTRY_AMOUNT     DECIMAL(15, 2),
    SOURCE_CURRENCY  INTEGER,
    FIXING           DECIMAL(12, 6),
    EUR_ENTRY_AMOUNT DECIMAL(15, 2)
);

