create table EXCESS_LMT_73395
(
    BANK_SECTOR        INTEGER,
    CUSTOMER_NAME      VARCHAR(90)    not null,
    ACCOUNT_NO         CHAR(16)       not null
        constraint PK_ACCID
            primary key,
    ACCOUNT_CURRENCY   CHAR(5)        not null,
    ACCOUNT_UNIT       INTEGER        not null,
    ACCOUNT_LIMIT      DECIMAL(15, 2) not null,
    TEMPORARY_EXCESS   DECIMAL(15, 2) not null,
    ENCROACH_TOLERANCE DECIMAL(15, 2) not null,
    BOOK_BALANCE       DECIMAL(15, 2),
    UNCLEAR_BALANCE    DECIMAL(15, 2),
    BLOCKED_BALANCE    DECIMAL(15, 2),
    EXCESS_AMNT        DECIMAL(15, 2) not null,
    LAST_TRX_DATE      DATE,
    START_EXCESS_DT    DATE           not null,
    AVERAGE_VALEUR_BAL DECIMAL(15, 2) not null,
    TEMP_EXC_START_DT  DATE,
    TEMP_EXC_END_DT    DATE,
    COMMENTS           VARCHAR(80),
    ACC_STATUS         CHAR(20)       not null,
    DEFINITE_DELAY     CHAR(3)        not null,
    CUST_CLASSIF       CHAR(2),
    CUST_PHONE         CHAR(15),
    MONITORING_EMPLOYE VARCHAR(90),
    CREDIT_AMOUNT      DECIMAL(15, 2) not null,
    STATUS             CHAR(1)
);

