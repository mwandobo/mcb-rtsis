create table BOT_72_CONTRACTDATES
(
    CONTRACTDATES_ID INTEGER generated always as identity
        constraint BOT_72_CONTRACTDATES_ID_PK
            primary key,
    EXPECTEDEND      DATE,
    LASTPAYMENT      DATE,
    REALEND          DATE,
    START            DATE,
    LOAN_CODE        VARCHAR(40),
    REPORTING_DATE   DATE
);

