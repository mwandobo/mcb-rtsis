create table DEP_MONTHLY_CR_DB
(
    ACTUAL_YEAR    SMALLINT    not null,
    ACTUAL_MONTH   SMALLINT    not null,
    ACCOUNT_NUMBER DECIMAL(11) not null,
    CREDITS        DECIMAL(15, 2),
    DEBITS         DECIMAL(15, 2),
    CR_COUNT       DECIMAL(15),
    DB_COUNT       DECIMAL(15),
    TMSTAMP        TIMESTAMP(6),
    constraint PK_DEP_MONTH_CR_DB
        primary key (ACCOUNT_NUMBER, ACTUAL_MONTH, ACTUAL_YEAR)
);

