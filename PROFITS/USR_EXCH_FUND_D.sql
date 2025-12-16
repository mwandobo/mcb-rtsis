create table USR_EXCH_FUND_D
(
    NOTES_COINS_TYPE  CHAR(1)        not null,
    DENOMINATION      DECIMAL(10, 2) not null,
    DB_CR             CHAR(1)        not null,
    ITEMS             DECIMAL(15)    not null,
    BALANCE           DECIMAL(15, 2) not null,
    FK_UE_USR_SEND    CHAR(8)        not null,
    FK_UE_USR_RECEIV  CHAR(8)        not null,
    FK_UE_ID_CURRENCY INTEGER        not null,
    FK_UE_TRX_UNIT    INTEGER        not null,
    FK_UE_TRX_DATE    DATE           not null,
    FK_UE_TRX_ID      INTEGER        not null,
    TMSTAMP           TIMESTAMP(6)   not null,
    constraint PK_EXCH_FUND_D
        primary key (FK_UE_USR_SEND, FK_UE_USR_RECEIV, FK_UE_ID_CURRENCY, FK_UE_TRX_UNIT, FK_UE_TRX_DATE, FK_UE_TRX_ID,
                     DB_CR, DENOMINATION, NOTES_COINS_TYPE)
);

