create table CURR_SPECIAL_RATE
(
    DEAL_SLIP_NUM      DECIMAL(10),
    TRN_SNUM           INTEGER,
    FK_UNITCODE        INTEGER,
    FK_CURRENCYID_CURR INTEGER,
    FK0UNITCODE        INTEGER,
    SELL_RATE          INTEGER,
    BUY_RATE           INTEGER,
    TRN_DATETIME       TIMESTAMP(6),
    VALEUR_DATE        DATE,
    TIMESTMP           TIMESTAMP(6),
    STATUS             CHAR(1),
    FK_USRCODE         CHAR(8)
);

create unique index IXU_CUR_006
    on CURR_SPECIAL_RATE (DEAL_SLIP_NUM);

