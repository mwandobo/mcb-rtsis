create table FX_OPEN_POS_HDR
(
    TRX_DATE        DATE           not null,
    TRX_UNIT        INTEGER        not null,
    TRX_USR         CHAR(8)        not null,
    TRX_SN          INTEGER        not null,
    TRX_INT_SN      SMALLINT       not null,
    TRX_AMOUNT      DECIMAL(15, 2),
    ID_CURRENCY     INTEGER        not null,
    UTILIZED_AMOUNT DECIMAL(15, 2),
    ENTRY_STATUS    CHAR(1)        not null,
    TMPSTAMP        DATE           not null,
    CUST_ID         INTEGER        not null,
    GLOBAL_ORDER_ID INTEGER,
    FIXING_RATE     DECIMAL(15, 8),
    BUY_SELL_RATE   DECIMAL(15, 8),
    BUY_SELL_IND    CHAR(1),
    LC_AMOUNT       DECIMAL(15, 2),
    POSITION_PROFIT DECIMAL(15, 2) not null,
    TARGET_CURRENCY INTEGER,
    constraint PK_FX_OPEN_POS_DTL
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TRX_INT_SN, ID_CURRENCY, ENTRY_STATUS)
);

