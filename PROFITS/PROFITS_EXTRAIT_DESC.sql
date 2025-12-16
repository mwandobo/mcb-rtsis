create table PROFITS_EXTRAIT_DESC
(
    AMOUNT_TYPE        CHAR(4)             not null,
    DEBIT_CREDIT       CHAR(1)             not null,
    TRX_CODE           INTEGER             not null,
    TRX_JUSTIFIC       INTEGER             not null,
    TRANSACTION_DESC   CHAR(40),
    JUSTIFICATION_DESC CHAR(40),
    DYNAMIC_DESCR_F    CHAR(1)             not null,
    PRINT_DOC_TYPE     CHAR(1) default '0' not null,
    constraint PKPROFITS_E
        primary key (AMOUNT_TYPE, DEBIT_CREDIT, TRX_CODE, TRX_JUSTIFIC, DYNAMIC_DESCR_F, PRINT_DOC_TYPE)
);

create unique index IX_DESC
    on PROFITS_EXTRAIT_DESC (AMOUNT_TYPE, DEBIT_CREDIT, TRX_CODE, TRX_JUSTIFIC, DYNAMIC_DESCR_F);

