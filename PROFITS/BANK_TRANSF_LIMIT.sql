create table BANK_TRANSF_LIMIT
(
    LIM_TYPE    CHAR(1)        not null,
    DAILY_LIMIT DECIMAL(15, 2) not null,
    MONTH_FREQ  INTEGER,
    START_DATE  DATE,
    LIMIT       DECIMAL(15, 2),
    constraint PK_BANK_TRANSFER_LIMIT
        primary key (DAILY_LIMIT, LIM_TYPE)
);

