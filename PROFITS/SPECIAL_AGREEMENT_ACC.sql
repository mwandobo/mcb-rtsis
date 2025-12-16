create table SPECIAL_AGREEMENT_ACC
(
    CUST_ID            INTEGER           not null,
    ID_TRANSACT        INTEGER           not null,
    ID_JUSTIFIC        INTEGER           not null,
    ID_PRODUCT         INTEGER           not null,
    ACCOUNT_NUMBER     INTEGER           not null,
    ID_CHANNEL         INTEGER default 0 not null,
    CHARGES_DISCOUNT   DECIMAL(8, 4),
    COMM_DISCOUNT      DECIMAL(8, 4),
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    FK_GENERIC_DETAFKH CHAR(5),
    FK_GENERIC_DETASER INTEGER
);

create unique index PKX_SPECIAL_AGREEMENT_ACC
    on SPECIAL_AGREEMENT_ACC (CUST_ID, ID_TRANSACT, ID_JUSTIFIC, ID_PRODUCT, ACCOUNT_NUMBER, ID_CHANNEL);

