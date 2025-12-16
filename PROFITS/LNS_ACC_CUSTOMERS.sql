create table LNS_ACC_CUSTOMERS
(
    CUST_ID       DECIMAL(7) not null,
    INSTANCE_NO   CHAR(5)    not null,
    PRIORITY_SN   DECIMAL(5),
    PROCESSED_FLG CHAR(1),
    constraint PK_LNS_ACC_CUSTOMERS
        primary key (CUST_ID, INSTANCE_NO)
);

