create table BLK_CUST_SALARY
(
    CUST_ID   INTEGER     not null,
    TRX_DATE  DATE        not null,
    SN        DECIMAL(12) not null,
    AMOUNT    DECIMAL(15, 2),
    FILE_NAME VARCHAR(50),
    FILE_HASH VARCHAR(50),
    TIMESTAMP TIMESTAMP(6),
    primary key (CUST_ID, TRX_DATE, SN)
);

