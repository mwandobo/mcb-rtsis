create table USER_TOTAL_DENOMIN
(
    DB_CR            CHAR(1)        not null,
    ITEMS            DECIMAL(15)    not null,
    BALANCE          DECIMAL(15, 2) not null,
    FK_ID_CURRENCY   INTEGER        not null,
    FK_DENOMINATION  DECIMAL(10, 2) not null,
    FK_NOTES_COINS   CHAR(1)        not null,
    FK_UT_CURRENCY   INTEGER        not null,
    FK_UT_USER       CHAR(8)        not null,
    FK_UT_REFER_UNIT INTEGER        not null,
    FK_UT_CORR_UNIT  INTEGER        not null,
    FK_UT_TRX_DATE   DATE           not null,
    FK_UT_SYSTEM     INTEGER        not null,
    TMSTAMP          TIMESTAMP(6)   not null,
    constraint PK_UT_DENOM
        primary key (FK_ID_CURRENCY, FK_DENOMINATION, FK_NOTES_COINS, FK_UT_CURRENCY, FK_UT_USER, FK_UT_REFER_UNIT,
                     FK_UT_CORR_UNIT, FK_UT_TRX_DATE, FK_UT_SYSTEM, DB_CR)
);

