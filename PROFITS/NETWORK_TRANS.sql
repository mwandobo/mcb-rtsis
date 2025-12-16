create table NETWORK_TRANS
(
    TRX_UNIT       INTEGER  not null,
    TRX_DATE       DATE     not null,
    TRX_USR        SMALLINT not null,
    TRX_SN         INTEGER  not null,
    O_ACCOUNT_UNIT INTEGER,
    O_TOTAL_AMOUNT DECIMAL(15, 2),
    LOAN_STATUS    CHAR(1),
    ACCOUNT_NUMBER DECIMAL(13),
    constraint PKEY000
        primary key (TRX_UNIT, TRX_DATE, TRX_USR, TRX_SN)
);

