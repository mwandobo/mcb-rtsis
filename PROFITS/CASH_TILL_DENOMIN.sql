create table CASH_TILL_DENOMIN
(
    DB_CR            CHAR(1)        not null,
    ITEMS            DECIMAL(15)    not null,
    BALANCE          DECIMAL(15, 2) not null,
    TMSATMP          TIMESTAMP(6)   not null,
    FK_ID_CURRENCY   INTEGER        not null,
    FK_DENOMINATION  DECIMAL(10, 2) not null,
    FK_NOTES_COINS   CHAR(1)        not null,
    FK_TILL_CURRENCY INTEGER        not null,
    FK_TILL_NO       INTEGER        not null,
    FK_TILL_UNIT     INTEGER        not null,
    constraint PK_CASH_DENOM
        primary key (FK_ID_CURRENCY, FK_DENOMINATION, FK_NOTES_COINS, FK_TILL_CURRENCY, FK_TILL_NO, FK_TILL_UNIT, DB_CR)
);

