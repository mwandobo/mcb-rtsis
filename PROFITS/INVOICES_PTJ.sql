create table INVOICES_PTJ
(
    ID_JUSTIFIC  INTEGER not null,
    ID_TRANSACT  INTEGER not null,
    ID_PRODUCT   INTEGER not null,
    INVOICE_TYPE CHAR(1),
    ENTRY_STATUS CHAR(1),
    constraint INVOICES_PTJ_PK
        primary key (ID_PRODUCT, ID_TRANSACT, ID_JUSTIFIC)
);

