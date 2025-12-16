create table TRA_PTJ_TAXABLE
(
    ID_PRODUCT        INTEGER not null,
    ID_TRANSACT       INTEGER not null,
    ID_JUSTIFIC       INTEGER not null,
    ID_COMMISSION     INTEGER,
    TAXABLE_FLG       CHAR(1),
    TRA_RECORD_METHOD VARCHAR(80),
    TMSTAMP           TIMESTAMP(6),
    constraint PK_TRA_PTJ_TXBL
        primary key (ID_PRODUCT, ID_JUSTIFIC, ID_TRANSACT)
);

comment on table TRA_PTJ_TAXABLE is 'Table which shows which Transactions/Jusitfications will be included in the taxable transactions for the TRA operation.';

