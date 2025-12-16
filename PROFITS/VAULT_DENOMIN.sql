create table VAULT_DENOMIN
(
    DB_CR             CHAR(1)        not null,
    ITEMS             DECIMAL(15)    not null,
    BALANCE           DECIMAL(15, 2) not null,
    FK_ID_CURRENCY    INTEGER        not null,
    FK_DENOMINATION   DECIMAL(10, 2) not null,
    FK_NOTES_COINS    CHAR(1)        not null,
    FK_VAULT_CURRENCY INTEGER        not null,
    FK_VAULT_UNIT     INTEGER        not null,
    TMSTAMP           TIMESTAMP(6)   not null,
    constraint PK_VAULT_DENOM
        primary key (FK_ID_CURRENCY, FK_DENOMINATION, FK_NOTES_COINS, FK_VAULT_CURRENCY, FK_VAULT_UNIT, DB_CR)
);

