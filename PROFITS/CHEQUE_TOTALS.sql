create table CHEQUE_TOTALS
(
    CHEQUE_STATUS      CHAR(2),
    CHEQUE_TYPE        CHAR(2),
    FK_BELONG_CURRENCY INTEGER,
    FK_MOVE_USR        CHAR(8),
    TRX_DATE           DATE,
    ITEMS              INTEGER,
    AMOUNT             DECIMAL(15, 2),
    AMOUNT_LC          DECIMAL(15, 2)
);

create unique index IXU_CHE_003
    on CHEQUE_TOTALS (CHEQUE_STATUS, CHEQUE_TYPE, FK_BELONG_CURRENCY, FK_MOVE_USR, TRX_DATE);

