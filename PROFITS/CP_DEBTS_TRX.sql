create table CP_DEBTS_TRX
(
    TRX_USR_SN     INTEGER not null,
    TRX_USR        CHAR(8) not null,
    TRX_DATE       DATE    not null,
    TRX_UNIT       INTEGER not null,
    CP_DEBT_AGR_NO DECIMAL(10),
    CP_DEBT_INT_SN DECIMAL(13),
    TRX_AMNT       DECIMAL(15, 2),
    CP_DEBT_DATE   DATE,
    constraint IXU_CP_081
        primary key (TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

