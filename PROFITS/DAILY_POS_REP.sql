create table DAILY_POS_REP
(
    ID_CURRENCY      INTEGER  not null,
    TRX_DATE         DATE     not null,
    BANK_CODE        SMALLINT not null,
    TOTAL_DOM_POS    DECIMAL(15, 2),
    SPOT_SELL        DECIMAL(15, 2),
    FWD_BUY          DECIMAL(15, 2),
    FWD_SELL         DECIMAL(15, 2),
    TOTAL_TRN_POS    DECIMAL(15, 2),
    TOTAL_PREV_DAY   DECIMAL(15, 2),
    TOTAL_INTERM_POS DECIMAL(15, 2),
    TOTAL_FX_POS     DECIMAL(15, 2),
    NXTD_SELL        DECIMAL(15, 2),
    NXTD_BUY         DECIMAL(15, 2),
    SMD_SELL         DECIMAL(15, 2),
    SMD_BUY          DECIMAL(15, 2),
    SPOT_BUY         DECIMAL(15, 2),
    CURR_DESC        CHAR(5),
    constraint IXU_REP_045
        primary key (ID_CURRENCY, TRX_DATE, BANK_CODE)
);

