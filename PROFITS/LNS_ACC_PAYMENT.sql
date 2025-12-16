create table LNS_ACC_PAYMENT
(
    LNS_OPEN_UNIT  INTEGER  not null,
    LNS_TYPE       SMALLINT not null,
    LNS_SN         INTEGER  not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    TRX_DATE       DATE,
    PROCESS_FLG    CHAR(1),
    LOCK_POSSIBLE  CHAR(1),
    INSTANCE_NO    CHAR(5),
    PRIORITY_SN    INTEGER,
    constraint PK_LNS_ACC_PAY
        primary key (LNS_SN, LNS_TYPE, LNS_OPEN_UNIT)
);

