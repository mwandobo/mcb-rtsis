create table CENTELOANS_MOBILE
(
    TRX_DATE                  DATE        not null,
    SERIAL_NUMBER             DECIMAL(15) not null,
    PROGRAM_ID                CHAR(5)     not null,
    CUST_ID                   INTEGER,
    ACCOUNT_NUMBER            CHAR(40),
    MONOTORING_UNIT           INTEGER,
    FIRST_NAME                CHAR(20),
    OTHER_NAME                VARCHAR(15),
    SURNAME                   CHAR(70),
    MOBILE_TEL                VARCHAR(15),
    ID_TYPE                   VARCHAR(40),
    ID_NUMBER                 CHAR(20),
    FCS_NUMBER                CHAR(20),
    DATE_OF_BIRTH             DATE,
    ADDRESS                   VARCHAR(40),
    GENDER                    CHAR(1),
    ACC_OPEN_DT               DATE,
    LOANAMOUNT                DECIMAL(15, 2),
    DAYSINARREARS             INTEGER,
    LOAN_STATUS               CHAR(1),
    LOAN_EXPIRY_DATE          DATE,
    LOAN_FINAL_CLASSIFICATION CHAR(1),
    SALARY_AMOUNT             DECIMAL(15, 2),
    AVERAGE_ACCOUNT_BALANCE   DECIMAL(15, 2),
    TMSTAMP                   TIMESTAMP(6),
    PROFFES_STATUS            VARCHAR(40),
    PRIMARY_OCCUPATION        VARCHAR(40),
    EMPLOYER                  VARCHAR(40),
    constraint PK_CENTELOANS_MOBILE
        primary key (PROGRAM_ID, SERIAL_NUMBER, TRX_DATE)
);

