create table ATM_DAILY_LIMIT
(
    LIM_TYPE      CHAR(1)        not null,
    DAILY_LIMIT   DECIMAL(15, 2) not null,
    DAYS_BACK     INTEGER,
    START_DATE    DATE,
    MONTHLY_LIMIT DECIMAL(15, 2),
    constraint PK_ATM_DAILY_LIMIT
        primary key (DAILY_LIMIT, LIM_TYPE)
);

