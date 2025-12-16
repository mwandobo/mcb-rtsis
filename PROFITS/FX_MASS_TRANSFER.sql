create table FX_MASS_TRANSFER
(
    TUN                CHAR(40) not null,
    TUN_INTERNAL_SN    INTEGER  not null,
    DEBIT_ACCOUNT_NUMB DECIMAL(11),
    CREDIT_ACCOUNT_NUM DECIMAL(11),
    AMOUNT             DECIMAL(15, 2),
    ENTRY_COMMENTS     CHAR(40),
    ENTRY_STATUS       CHAR(1),
    DR_JUSTIFIC        INTEGER,
    CR_JUSTIFIC        INTEGER,
    TRX_COUNTER        DECIMAL(10),
    ERROR_MSG          CHAR(80),
    constraint IXU_FX__006
        primary key (TUN_INTERNAL_SN, TUN)
);

