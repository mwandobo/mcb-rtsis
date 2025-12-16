create table BOT_2_SUBJECTCHOICE
(
    SUBJECTCHOICE_ID     INTEGER generated always as identity
        constraint BOT_2_SUBJECTCHOICE_ID_PK
            primary key,
    FK_BOT_10_COMPANY    INTEGER
        constraint FK_BOT_2_BOT_10__
            references BOT_10_COMPANY,
    FK_BOT_11_INDIVIDUAL INTEGER
        constraint FK_BOT_2_BOT_11__
            references BOT_11_INDIVIDUAL,
    CUST_ID              VARCHAR(32),
    LOAN_CODE            VARCHAR(40),
    REPORTING_DATE       DATE
);

