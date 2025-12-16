create table BOV_TRX_RECORDING
(
    TRX_UNIT       INTEGER not null,
    TRX_DATE       DATE    not null,
    TRX_USR        CHAR(8) not null,
    TRX_SN         INTEGER not null,
    I_AMOUNT_1     DECIMAL(15, 2),
    I_TRX_COMMENTS CHAR(40),
    constraint PK_BOV_TRX_RECORDI
        primary key (TRX_UNIT, TRX_USR, TRX_DATE, TRX_SN)
);

