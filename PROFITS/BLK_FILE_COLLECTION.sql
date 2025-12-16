create table BLK_FILE_COLLECTION
(
    TEMPLATE_ID       INTEGER           not null,
    SN                INTEGER           not null,
    ORDER_SN          INTEGER           not null,
    PRFT_SYSTEM       SMALLINT          not null,
    ID_TRANSACT       INTEGER           not null,
    ID_JUSTIFIC       INTEGER           not null,
    ID_PRODUCT        INTEGER default 0 not null,
    LOAN_GROUP_NUMBER INTEGER,
    DECISION          CHAR(1),
    constraint PK_BLK_FILE_COLLECTION
        primary key (TEMPLATE_ID, SN)
);

