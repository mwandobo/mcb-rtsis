create table RSKCO_BANKACCOUNTS
(
    ACCOUNT_NUMBER     CHAR(20) not null,
    BANK_NAME          CHAR(50) not null,
    IS_ACTIVE          SMALLINT,
    PRFT_EXTRACTION_DT DATE,
    OPEN_DATE          DATE,
    CURRENCY           CHAR(3),
    ACCOUNT_TYPE       CHAR(10),
    PRFT_ROUTINE       CHAR(20),
    OWNER              CHAR(30),
    LOAN_REFERENCE     CHAR(30),
    NOTES              CHAR(100),
    constraint IXU_LNS_039
        primary key (ACCOUNT_NUMBER, BANK_NAME)
);

