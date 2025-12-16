create table MOF_ACCOUNT_TOTAL
(
    CUST_ID        INTEGER     not null,
    ACCOUNT_NUMBER DECIMAL(11) not null,
    TOTAL_AMOUNT   DECIMAL(15, 2),
    constraint PK_MOF_ACCOUNT_TOTAL
        primary key (ACCOUNT_NUMBER, CUST_ID)
);

