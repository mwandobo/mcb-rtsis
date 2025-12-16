create table DOF_MNT_RECORDING
(
    TRX_DATE         DATE       not null,
    TRX_UNIT         INTEGER    not null,
    TRX_USR          CHAR(8)    not null,
    TRX_SN           DECIMAL(8) not null,
    TRX_INTERNAL_SN  SMALLINT   not null,
    TRX_CODE         INTEGER,
    CUSTOMER         DECIMAL(7),
    CP_AGREEMENT_NO  DECIMAL(10),
    H_START_DATE     DATE,
    H_SUSPEND_DATE   DATE,
    H_TOTAL_AMOUNT   DECIMAL(15, 2),
    H_ENTRY_STATUS   CHAR(1),
    D_ACCOUNT_NUMBER CHAR(40),
    D_PRFT_SYSTEM    SMALLINT,
    D_PRIORITY_SN    INTEGER,
    D_AMOUNT         DECIMAL(15, 2),
    D_DEDUCTION_CODE CHAR(3),
    D_ENTRY_STATUS   CHAR(1),
    TMSTAMP          TIMESTAMP(6),
    constraint I0000965
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TRX_INTERNAL_SN)
);

