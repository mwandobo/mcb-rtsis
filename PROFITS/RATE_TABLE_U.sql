create table RATE_TABLE_U
(
    EXCH_NOTES_FLAG    CHAR(1)        not null,
    FK_CURRENCYID_CURR INTEGER        not null,
    FK_CUSTOMERCUST_ID INTEGER        not null,
    FK_GENERIC_DETAFK  CHAR(5)        not null,
    FK_GENERIC_DETASER INTEGER        not null,
    RATE_TABLE_NUMBER  DECIMAL(10)    not null,
    SCALE_MAX_AMOUNT   DECIMAL(15, 2) not null,
    ACTIVATION_DATE    DATE           not null,
    MULTIPLIER         INTEGER,
    BUY_RATE           DECIMAL(12, 6) not null,
    FIXING_RATE        DECIMAL(12, 6) not null,
    SELL_RATE          DECIMAL(12, 6) not null,
    MIN_NEGOT_UNIT     SMALLINT,
    TMSTAMP            TIMESTAMP(6)   not null,
    BUY_RATE_MT        DECIMAL(12, 6),
    ACTIVATION_TIME    TIME,
    constraint IXU_CIU_070
        primary key (EXCH_NOTES_FLAG, FK_CURRENCYID_CURR, FK_CUSTOMERCUST_ID, FK_GENERIC_DETAFK, FK_GENERIC_DETASER,
                     RATE_TABLE_NUMBER, SCALE_MAX_AMOUNT)
);

