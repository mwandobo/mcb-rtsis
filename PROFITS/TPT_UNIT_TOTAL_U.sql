create table TPT_UNIT_TOTAL_U
(
    TRX_UNIT      INTEGER        not null,
    TPT_TASK      CHAR(8)        not null,
    TP_TYPE       CHAR(5)        not null,
    TP_NUMBER     DECIMAL(15)    not null,
    ACTUAL_YEAR   SMALLINT       not null,
    ACTUAL_MONTH  SMALLINT       not null,
    TRX_CURRENCY  INTEGER        not null,
    DB_NET_AMOUNT DECIMAL(15, 2),
    DB_CHR_AMOUNT DECIMAL(15, 2),
    DB_TAX_AMOUNT DECIMAL(15, 2),
    DB_COM_AMOUNT DECIMAL(15, 2),
    CR_NET_AMOUNT DECIMAL(15, 2),
    CR_TAX_AMOUNT DECIMAL(15, 2),
    CR_CHR_AMOUNT DECIMAL(15, 2),
    CR_COM_AMOUNT DECIMAL(15, 2),
    CHR_IN_DC     DECIMAL(15, 2),
    PREV_BALANCE  DECIMAL(15, 2) not null,
    constraint IXU_CIU_059
        primary key (ACTUAL_MONTH, ACTUAL_YEAR, TP_NUMBER, TP_TYPE, TPT_TASK, TRX_UNIT)
);

