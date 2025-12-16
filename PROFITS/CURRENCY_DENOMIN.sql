create table CURRENCY_DENOMIN
(
    NOTES_COINS_TYPE CHAR(1)        not null,
    DENOMINATION     DECIMAL(10, 2) not null,
    SHORT_DESCR      CHAR(25)       not null,
    DESCRIPTION      VARCHAR(40),
    FK_ID_CURRENCY   INTEGER        not null,
    STRAPS_ROLLS_1   DECIMAL(10),
    STRAPS_ROLLS_2   DECIMAL(10),
    STRAPS_ROLLS_3   DECIMAL(10),
    constraint PK_CURR_DENOM
        primary key (FK_ID_CURRENCY, DENOMINATION, NOTES_COINS_TYPE)
);

