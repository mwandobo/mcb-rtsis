create table CUST_GLOBAL_COMM
(
    CUST_ID           INTEGER not null
        constraint PK_CUST_GLOBAL_COMM
            primary key,
    AGREED_INCOME_AMN DECIMAL(18, 2),
    DURATION_NUMBER   SMALLINT,
    DURATION_UNIT     CHAR(1),
    ENTRY_STATUS      CHAR(1),
    TMSTAMP           TIMESTAMP(6),
    ACCOUNT_NUMBER    CHAR(40),
    C_DIGIT           SMALLINT,
    PRFT_SYSTEM       SMALLINT
);

comment on column CUST_GLOBAL_COMM.ENTRY_STATUS is '0 Inactive, 1 Active';

