create table LC_LIMITS
(
    LC_ACCOUNT_NUMBER CHAR(40) not null
        constraint IXU_FX_016
            primary key,
    ID_CURRENCY       INTEGER,
    HISTORY_CNT       DECIMAL(10),
    LC_UTILIZED_LIMIT DECIMAL(15, 2),
    LC_LIMIT_AMOUNT   DECIMAL(15, 2)
);

