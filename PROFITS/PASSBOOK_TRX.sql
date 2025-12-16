create table PASSBOOK_TRX
(
    FK_DEPOSIT_ACCOACC DECIMAL(11) not null,
    PSB_LINE_SN        INTEGER     not null,
    PSB_PRINTED_LINE   SMALLINT,
    TRX_UNIT           INTEGER,
    ID_JUSTIFIC        INTEGER,
    ID_TRANSACT        INTEGER,
    TRX_USR_SN         INTEGER,
    AMOUNT             DECIMAL(15, 2),
    PREV_LINE_BALANCE  DECIMAL(15, 2),
    TRX_DATE           DATE,
    REVERSE_FLAG       CHAR(1),
    EURO_FLAG          CHAR(1),
    DEBIT_CREDIT_FLAG  CHAR(1),
    PSB_CODE           CHAR(3),
    TRX_USR            CHAR(8),
    constraint IXU_PAS_000
        primary key (FK_DEPOSIT_ACCOACC, PSB_LINE_SN)
);

