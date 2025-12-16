create table GLOBAL_TRIPLET
(
    PRODUCT_ID       INTEGER  not null,
    TRANSACTION_ID   INTEGER  not null,
    JUSTIFICATION_ID INTEGER  not null,
    TAG_SET_CODE     CHAR(20) not null,
    USED_FLG         CHAR(1),
    constraint PKX_TRIPLET_PARFLD
        primary key (JUSTIFICATION_ID, TRANSACTION_ID, PRODUCT_ID)
);

