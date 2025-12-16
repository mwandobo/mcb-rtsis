create table LOAN_LIQUIDITY
(
    PRFT_SYSTEM    SMALLINT not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    MONTH_11       DECIMAL(15, 2),
    MONTH_1        DECIMAL(15, 2),
    MONTH_2        DECIMAL(15, 2),
    MONTH_3        DECIMAL(15, 2),
    MONTH_4        DECIMAL(15, 2),
    MONTH_5        DECIMAL(15, 2),
    MONTH_6        DECIMAL(15, 2),
    MONTH_7        DECIMAL(15, 2),
    MONTH_8        DECIMAL(15, 2),
    MONTH_9        DECIMAL(15, 2),
    MONTH_10       DECIMAL(15, 2),
    REMAINING_AMN  DECIMAL(15, 2),
    MONTH_12       DECIMAL(15, 2),
    YEAR_2         DECIMAL(15, 2),
    YEAR_3         DECIMAL(15, 2),
    YEAR_4         DECIMAL(15, 2),
    YEAR_5         DECIMAL(15, 2),
    YEAR_6         DECIMAL(15, 2),
    YEAR_7         DECIMAL(15, 2),
    YEAR_8         DECIMAL(15, 2),
    YEAR_9         DECIMAL(15, 2),
    YEAR_10        DECIMAL(15, 2),
    YEAR_11        DECIMAL(15, 2),
    YEAR_12        DECIMAL(15, 2),
    constraint IXU_LNS_036
        primary key (PRFT_SYSTEM, ACCOUNT_NUMBER)
);

