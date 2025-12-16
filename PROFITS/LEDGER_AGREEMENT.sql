create table LEDGER_AGREEMENT
(
    TRX_DATE     DATE     not null,
    SUBSYSTEM    CHAR(2)  not null,
    UNIT_CODE    SMALLINT not null,
    GL_ACCOUNT   CHAR(21) not null,
    CURRENCY_ID  INTEGER  not null,
    APPL_BALANCE DECIMAL(13, 2),
    LEDG_BALANCE DECIMAL(13, 2),
    LEDG_DEBIT   DECIMAL(13, 2),
    LEDG_CREDIT  DECIMAL(13, 2),
    constraint I
        primary key (TRX_DATE, SUBSYSTEM, GL_ACCOUNT, CURRENCY_ID, UNIT_CODE)
);

