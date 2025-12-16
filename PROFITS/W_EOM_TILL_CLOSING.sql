create table W_EOM_TILL_CLOSING
(
    EOM_DATE               DATE       not null,
    UNIT_CODE              DECIMAL(5) not null,
    USER_CODE              CHAR(8)    not null,
    CURRENCY_ID            DECIMAL(5),
    SUBSYSTEM              DECIMAL(5) not null,
    TILL_TIMESTAMP         TIMESTAMP(6),
    USER_NAME              VARCHAR(41),
    TILL_NUMBER            DECIMAL(5),
    CURRENCY_CODE          VARCHAR(5) not null,
    CASH_CREDIT            DECIMAL(15, 2),
    CASH_DEBIT             DECIMAL(15, 2),
    CASH_DIFFERENCE        DECIMAL(15, 2),
    JOURNAL_CREDIT         DECIMAL(15, 2),
    JOURNAL_DEBIT          DECIMAL(15, 2),
    JOURNAL_DIFFERENCE     DECIMAL(15, 2),
    ROW_CREATION_TIMESTAMP TIMESTAMP(6) default CURRENT TIMESTAMP,
    primary key (EOM_DATE, UNIT_CODE, USER_CODE, SUBSYSTEM, CURRENCY_CODE)
);

