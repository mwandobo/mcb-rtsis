create table VAULT
(
    FK_CURRENCYID_CURR INTEGER,
    FK_UNITCODE        INTEGER,
    AMOUNT             DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6)
);

create unique index IXU_VAU_000
    on VAULT (FK_CURRENCYID_CURR, FK_UNITCODE);

