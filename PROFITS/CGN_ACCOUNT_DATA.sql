create table CGN_ACCOUNT_DATA
(
    CGN_SN         DECIMAL(10) generated always as identity,
    ACC_VIEW_SN    DECIMAL(10) not null,
    ACC_CUST_ID    INTEGER,
    ACCOUNT_NUMBER CHAR(40),
    constraint PK_ACC_VIEW
        primary key (ACC_VIEW_SN, CGN_SN)
);

