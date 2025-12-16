create table ATM_DELTA_ACC_TRANS
(
    ACCOUNT_NUMBER DECIMAL(11) not null
        constraint ATM_DELTA_ACC_TRANS_PK
            primary key,
    TRANSACT_COUNT INTEGER,
    CUST_ID        INTEGER,
    TMSTAMP        TIMESTAMP(6),
    LAST_UPD_DATE  DATE
);

