create table LOANS_BATCH_BALANCE
(
    TRX_CODE       INTEGER not null,
    LNS_UNIT       INTEGER not null,
    LNS_TYPE       INTEGER not null,
    LNS_SN         INTEGER not null,
    ACCOUNT_NUMBER CHAR(40),
    TRX_DATE       DATE,
    PROCESS_FLG    CHAR(1),
    INSTANCE_NO    CHAR(5),
    CUST_ID        INTEGER,
    PRIORITY_SN    INTEGER,
    primary key (TRX_CODE, LNS_SN, LNS_TYPE, LNS_UNIT)
);

