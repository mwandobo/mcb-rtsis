create table LOAN_CUST_EXTRA
(
    CUST_ID        INTEGER      not null,
    ACCOUNT_NUMBER CHAR(40)     not null,
    PRFT_SYSTEM    SMALLINT     not null,
    PRINT_PATH     CHAR(254),
    TMSTAMP        TIMESTAMP(6) not null
);

create unique index IXU_CUSTEX
    on LOAN_CUST_EXTRA (CUST_ID, ACCOUNT_NUMBER, PRFT_SYSTEM, TMSTAMP);

alter table LOAN_CUST_EXTRA
    add constraint PK_CUSTEX
        primary key (CUST_ID, ACCOUNT_NUMBER, PRFT_SYSTEM, TMSTAMP);

