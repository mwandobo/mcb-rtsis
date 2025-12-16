create table DEP_CHEQUE_TOTAL
(
    ACH_BANK_CODE      CHAR(3) default '015',
    TRX_UNIT           INTEGER,
    TRX_DATE           DATE,
    TRX_USR            CHAR(8),
    ID_CURRENCY        INTEGER,
    TOTAL_ITEMS        SMALLINT,
    TOTAL_ITEMS_AMOUNT DECIMAL(15, 2),
    OUR_BANK_FLAG      CHAR(1)
);

create unique index IXU_DEP_032
    on DEP_CHEQUE_TOTAL (ACH_BANK_CODE, TRX_UNIT, TRX_DATE, TRX_USR, ID_CURRENCY);

