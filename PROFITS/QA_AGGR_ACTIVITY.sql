create table QA_AGGR_ACTIVITY
(
    WLT_YEAR        INTEGER not null,
    CUST            INTEGER not null,
    BASE_AMNT       DECIMAL(15, 2),
    BASE_AMNT_DEBIT DECIMAL(15, 2),
    primary key (CUST, WLT_YEAR)
);

