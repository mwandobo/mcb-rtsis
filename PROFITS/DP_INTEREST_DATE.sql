create table DP_INTEREST_DATE
(
    INTEREST_DATE  DATE        not null,
    ACCOUNT_NUMBER DECIMAL(11) not null,
    DB_INT_AMOUNT  DECIMAL(15, 2),
    constraint PK_DP_INT_DT
        primary key (INTEREST_DATE, ACCOUNT_NUMBER)
);

