create table CLEARING_SYSTEM
(
    SYSTEM_CODE        CHAR(10) not null
        constraint IXU_CLE_001
            primary key,
    FK_CURRENCYID_CURR INTEGER,
    FOREIGN_TITLES_CLR CHAR(1),
    CUSTODY_CODE       CHAR(10),
    SWIFT_ADDRESS      CHAR(14),
    BANK_CURRENT_ACCNT CHAR(21),
    SYSTEM_DESC        CHAR(40)
);

