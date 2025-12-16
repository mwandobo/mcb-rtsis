create table W_STG_TILL_CLOSING
(
    EOM_DATE               DATE,
    UNIT_CODE              INTEGER,
    USER_CODE              CHAR(8),
    CURRENCY_ID            INTEGER,
    SUBSYSTEM              INTEGER,
    USER_NAME              CHAR(41),
    TILL_NUMBER            INTEGER,
    CURRENCY_CODE          CHAR(5),
    CASH_CREDIT            DECIMAL(15, 2),
    CASH_DEBIT             DECIMAL(15, 2),
    CASH_DIFFERENCE        DECIMAL(15, 2),
    JOURNAL_CREDIT         DECIMAL(15, 2),
    JOURNAL_DEBIT          DECIMAL(15, 2),
    JOURNAL_DIFFERENCE     DECIMAL(15, 2),
    ROW_CREATION_TIMESTAMP TIMESTAMP(6)
);

