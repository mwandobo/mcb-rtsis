create table BOT_1_COMMAND
(
    COMMAND_ID     INTEGER generated always as identity
        constraint BOT_1_COMMAND_ID_PK
            primary key,
    IDENTIFIER     VARCHAR(32) not null,
    LOAN_CODE      VARCHAR(40),
    REPORTING_DATE DATE
);

