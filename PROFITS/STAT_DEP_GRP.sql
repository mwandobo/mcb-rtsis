create table STAT_DEP_GRP
(
    PRODUCT_ID    INTEGER  not null,
    DEPOSIT_GROUP SMALLINT not null,
    SN            INTEGER,
    constraint PKSTATDE
        primary key (DEPOSIT_GROUP, PRODUCT_ID)
);

